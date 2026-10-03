// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"testing"

	"github.com/syncthing/syncthing/lib/protocol"
)

func TestDrainFolderType(t *testing.T) {
	var ft FolderType
	if err := ft.UnmarshalText([]byte("drain")); err != nil {
		t.Fatal(err)
	}
	if ft != FolderTypeDrain || ft.String() != "drain" {
		t.Errorf("drain didn't round trip: %v", ft)
	}
}

func TestDrainOrderText(t *testing.T) {
	for _, o := range []DrainOrder{DrainOrderRandom, DrainOrderAlphabetic, DrainOrderSmallestFirst, DrainOrderLargestFirst, DrainOrderOldestFirst, DrainOrderNewestFirst} {
		var got DrainOrder
		if err := got.UnmarshalText([]byte(o.String())); err != nil || got != o {
			t.Errorf("%v didn't round trip: %v, %v", o, got, err)
		}
	}
	var got DrainOrder
	if err := got.UnmarshalText([]byte("nonsense")); err != nil || got != DrainOrderOldestFirst {
		t.Errorf("unknown orders should fall back to oldestFirst, got %v", got)
	}
}

func TestDrainSeedLevelMinimum(t *testing.T) {
	myID := protocol.DeviceID{1}
	cfg := New(myID)
	cfg.Folders = []FolderConfiguration{{ID: "f", Path: t.TempDir(), Type: FolderTypeDrain, DrainSeedLevel: 0}}
	if err := cfg.prepare(myID); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Folders[0].DrainSeedLevel; got != 1 {
		t.Errorf("seed level should be at least 1, got %d", got)
	}
}
