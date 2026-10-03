// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"slices"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func openDrainTestDB(t *testing.T) *DB {
	t.Helper()
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return sdb
}

func TestLocalVersionAvailability(t *testing.T) {
	t.Parallel()
	sdb := openDrainTestDB(t)

	local := genFile("a", 1, 0)
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{local}); err != nil {
		t.Fatal(err)
	}

	same := local
	invalid := local
	invalid.LocalFlags = protocol.FlagLocalRemoteInvalid
	other := local
	other.Version = local.Version.Update(44)
	deleted := local
	deleted.Deleted = true

	for dev, fi := range map[protocol.DeviceID]protocol.FileInfo{
		{42}: same,    // counts
		{43}: invalid, // the remote doesn't really have it
		{44}: other,   // a different version
		{45}: deleted, // same version, but deleted
	} {
		if err := sdb.Update(folderID, dev, []protocol.FileInfo{fi}); err != nil {
			t.Fatal(err)
		}
	}

	have, err := sdb.GetLocalVersionAvailability(folderID, "a")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(have, []protocol.DeviceID{{42}}) {
		t.Errorf("expected only device 42 to hold our version, got %v", have)
	}
}

func TestDrainCandidatesAndCounts(t *testing.T) {
	t.Parallel()
	sdb := openDrainTestDB(t)

	small := genFile("small", 1, 0)
	big := genFile("big", 3, 0)
	gone := genFile("gone", 1, 0)
	gone.Deleted = true
	gone.Blocks = nil
	gone.Size = 0
	drained := genFile("drained", 2, 0)
	drained.SetDrained()
	ignored := genFile("ignored", 1, 0)
	ignored.SetIgnored()

	files := []protocol.FileInfo{small, big, genDir("dir", 0), gone, drained, ignored}
	if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}

	names := func(order config.DrainOrder) []string {
		cands := mustCollect[db.FileMetadata](t)(sdb.AllLocalDrainCandidates(folderID, order))
		res := make([]string, len(cands))
		for i, c := range cands {
			res[i] = c.Name
		}
		return res
	}
	for order, want := range map[config.DrainOrder][]string{
		config.DrainOrderLargestFirst:  {"big", "small"},
		config.DrainOrderSmallestFirst: {"small", "big"},
		config.DrainOrderAlphabetic:    {"big", "small"},
		config.DrainOrderOldestFirst:   {"small", "big"}, // genFile times are increasing
		config.DrainOrderNewestFirst:   {"big", "small"},
	} {
		if got := names(order); !slices.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", order, got, want)
		}
	}

	// Local counts are what's on disk: no drained or ignored files.
	local, err := sdb.CountLocal(folderID, protocol.LocalDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if local.Files != 2 || local.Bytes != small.Size+big.Size {
		t.Errorf("unexpected local counts: %+v", local)
	}

	d, err := sdb.CountDrained(folderID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Files != 1 || d.Bytes != drained.Size {
		t.Errorf("unexpected drained counts: %+v", d)
	}
}
