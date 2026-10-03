// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// setupDrainFolder starts a model with a drain folder "dr" shared with
// device1 and device2, and lets mod adjust the folder config first.
func setupDrainFolder(t *testing.T, mod func(*config.FolderConfiguration)) (*testModel, *drainFolder) {
	t.Helper()

	w, cancel := newConfigWrapper(defaultCfg)
	t.Cleanup(cancel)
	cfg := w.RawCopy()
	cfg.SetDevice(newDeviceConfiguration(cfg.Defaults.Device, device2, "device2"))
	fcfg := newFolderConfig()
	fcfg.ID = "dr"
	fcfg.Label = "dr"
	fcfg.Type = config.FolderTypeDrain
	// Pulls, and so drains, only happen when a test calls runPull.
	fcfg.PullerDelayS = 3600
	fcfg.Devices = append(fcfg.Devices, config.FolderDeviceConfiguration{DeviceID: device2})
	if mod != nil {
		mod(&fcfg)
	}
	cfg.Folders = []config.FolderConfiguration{fcfg}
	replace(t, w, cfg)

	m := newModel(t, w, myID, nil)
	m.ServeBackground()
	t.Cleanup(func() { cleanupModel(m) })
	<-m.started
	// Finish the initial scan first, so it can't catch test files half
	// set up.
	must(t, m.ScanFolder("dr"))

	m.mut.RLock()
	r, _ := m.folderRunners.Get("dr")
	m.mut.RUnlock()
	return m, r.(*drainFolder)
}

// writeDrainFiles writes files of the given sizes, oldest first, and scans.
func writeDrainFiles(t *testing.T, m *testModel, f *drainFolder, sizes map[string]int, order ...string) {
	t.Helper()
	ffs := f.Filesystem()
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i, name := range order {
		writeFile(t, ffs, name, bytes.Repeat([]byte{'x'}, sizes[name]))
		mtime := base.Add(time.Duration(i) * time.Minute)
		must(t, ffs.Chtimes(name, mtime, mtime))
	}
	must(t, m.ScanFolder("dr"))
}

// announce makes dev claim our current version of each named file.
func announce(t *testing.T, m *testModel, conn *fakeConnection, names ...string) {
	t.Helper()
	files := make([]protocol.FileInfo, 0, len(names))
	for _, name := range names {
		fi, ok := m.testCurrentFolderFile("dr", name)
		if !ok {
			t.Fatalf("no local file %q", name)
		}
		fi.LocalFlags = 0
		files = append(files, prepareFileInfoForIndex(fi))
	}
	must(t, m.Index(conn, &protocol.Index{Folder: "dr", Files: files}))

	// Index processing finishes asynchronously; wait until the database
	// shows the device holding our version of every file.
	for _, name := range names {
		waitFor(t, func() bool {
			have := mustV(m.sdb.GetLocalVersionAvailability("dr", name))
			return slices.Contains(have, conn.DeviceID())
		})
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// runPull runs a pull, and so the drain step, on the folder's goroutine.
func runPull(t *testing.T, f *drainFolder) {
	t.Helper()
	must(t, f.doInSync(func(ctx context.Context) error {
		// The generic folder pull, not the send only puller step.
		_, err := f.folder.pull(ctx)
		return err
	}))
}

func onDisk(t *testing.T, f *drainFolder, name string) bool {
	t.Helper()
	_, err := f.Filesystem().Lstat(name)
	switch {
	case err == nil:
		return true
	case fs.IsNotExist(err):
		return false
	default:
		t.Fatal(err)
		return false
	}
}

func TestDrainRemovesSeededFiles(t *testing.T) {
	m, f := setupDrainFolder(t, nil)
	writeDrainFiles(t, m, f, map[string]int{"seeded": 100, "unseeded": 200}, "seeded", "unseeded")
	before, _ := m.testCurrentFolderFile("dr", "seeded")

	conn := addFakeConn(m, device1, "dr")
	announce(t, m, conn, "seeded")
	runPull(t, f)

	if onDisk(t, f, "seeded") {
		t.Error("seeded file should have been drained")
	}
	if !onDisk(t, f, "unseeded") {
		t.Error("unseeded file should still be on disk")
	}

	after, _ := m.testCurrentFolderFile("dr", "seeded")
	if !after.IsDrained() || after.IsDeleted() {
		t.Errorf("expected a drained, non-deleted entry, got %v", after)
	}
	if !after.Version.Equal(before.Version) {
		t.Errorf("draining must keep the version: before %v, after %v", before.Version, after.Version)
	}
	if !after.ToWire(false).Invalid {
		t.Error("drained file must be announced as invalid")
	}

	// We don't need it back, and the remote isn't asked to delete it; it
	// only still needs the file it never had.
	if need := mustV(m.NeedSize("dr", protocol.LocalDeviceID)); need.TotalItems() != 0 {
		t.Errorf("local device should need nothing, needs %+v", need)
	}
	if need := mustV(m.NeedSize("dr", device1)); need.Deleted != 0 || need.Files != 1 || need.Bytes != 200 {
		t.Errorf("remote should only need the unseeded file, needs %+v", need)
	}

	local := mustV(m.LocalSize("dr", protocol.LocalDeviceID))
	if local.Files != 1 || local.Bytes != 200 {
		t.Errorf("local size should only count the unseeded file: %+v", local)
	}
	drained := mustV(m.DrainedSize("dr"))
	if drained.Files != 1 || drained.Bytes != 100 {
		t.Errorf("expected one drained file of 100 bytes: %+v", drained)
	}
}

func TestDrainSeedLevelCountsConnectedDevices(t *testing.T) {
	m, f := setupDrainFolder(t, func(fcfg *config.FolderConfiguration) {
		fcfg.DrainSeedLevel = 2
	})
	writeDrainFiles(t, m, f, map[string]int{"a": 100, "b": 100}, "a", "b")

	conn1 := addFakeConn(m, device1, "dr")
	announce(t, m, conn1, "a", "b")
	runPull(t, f)
	if !onDisk(t, f, "a") || !onDisk(t, f, "b") {
		t.Fatal("one seed is below a seed level of two")
	}

	conn2 := addFakeConn(m, device2, "dr")
	announce(t, m, conn2, "a", "b")

	// device2 holds the files, but no longer counts once disconnected.
	conn2.Close(errors.New("test disconnect"))
	runPull(t, f)
	if !onDisk(t, f, "a") || !onDisk(t, f, "b") {
		t.Fatal("a disconnected device must not count as a seed")
	}

	conn2 = addFakeConn(m, device2, "dr")
	announce(t, m, conn2, "a", "b")
	runPull(t, f)
	if onDisk(t, f, "a") || onDisk(t, f, "b") {
		t.Fatal("two connected seeds should drain both files")
	}
}

func TestDrainIgnoresOtherVersionsAndUntrustedDevices(t *testing.T) {
	m, f := setupDrainFolder(t, func(fcfg *config.FolderConfiguration) {
		for i := range fcfg.Devices {
			if fcfg.Devices[i].DeviceID == device2 {
				fcfg.Devices[i].EncryptionPassword = "secret"
			}
		}
	})
	writeDrainFiles(t, m, f, map[string]int{"a": 100}, "a")

	// device1 announces a different version, with different content, of
	// the file. (Identical content would be merged into one version.)
	conn1 := addFakeConn(m, device1, "dr")
	fi, _ := m.testCurrentFolderFile("dr", "a")
	fi.Version = protocol.Vector{}.Update(device1.Short())
	fi.Size = 999
	fi.Blocks = []protocol.BlockInfo{{Hash: bytes.Repeat([]byte{1}, 32), Size: 999}}
	fi.BlocksHash = protocol.BlocksHash(fi.Blocks)
	must(t, m.Index(conn1, &protocol.Index{Folder: "dr", Files: []protocol.FileInfo{prepareFileInfoForIndex(fi)}}))
	runPull(t, f)
	if !onDisk(t, f, "a") {
		t.Fatal("a different version must not count as a seed")
	}

	if got := f.connectedTrustedDevices(); len(got) != 1 {
		t.Fatalf("expected only device1 as a trusted connected device, got %v", got)
	}
	addFakeConn(m, device2, "dr")
	if got := f.connectedTrustedDevices(); len(got) != 1 {
		t.Fatalf("an untrusted device must not count as a seed, got %v", got)
	}
}

func TestDrainWaterMarksAndOrder(t *testing.T) {
	m, f := setupDrainFolder(t, func(fcfg *config.FolderConfiguration) {
		fcfg.DrainHighWater = config.Size{Value: 250}
		fcfg.DrainLowWater = config.Size{Value: 150}
		fcfg.DrainOrder = config.DrainOrderOldestFirst
	})
	writeDrainFiles(t, m, f, map[string]int{"old": 100, "mid": 100, "new": 100}, "old", "mid", "new")

	conn := addFakeConn(m, device1, "dr")
	announce(t, m, conn, "old", "mid", "new")
	runPull(t, f)

	// 300 bytes is over the high mark; drain oldest first until at or
	// under the low mark of 150, which takes two files.
	if onDisk(t, f, "old") || onDisk(t, f, "mid") {
		t.Error("the two oldest files should have been drained")
	}
	if !onDisk(t, f, "new") {
		t.Error("the newest file should be kept")
	}

	// 100 bytes is under the high mark, so nothing more happens.
	runPull(t, f)
	if !onDisk(t, f, "new") {
		t.Error("nothing should be drained below the high mark")
	}
}

func TestDrainedFileSurvivesRescanAndCanBeRestored(t *testing.T) {
	m, f := setupDrainFolder(t, nil)
	writeDrainFiles(t, m, f, map[string]int{"a": 100}, "a")
	conn := addFakeConn(m, device1, "dr")
	announce(t, m, conn, "a")
	runPull(t, f)
	drained, _ := m.testCurrentFolderFile("dr", "a")

	must(t, m.ScanFolder("dr"))
	if fi, _ := m.testCurrentFolderFile("dr", "a"); !fi.IsDrained() || fi.IsDeleted() {
		t.Fatalf("a rescan must not turn a drained file into a deletion: %v", fi)
	}

	// Putting the file back makes it a regular local file again.
	writeFile(t, f.Filesystem(), "a", []byte("restored"))
	must(t, m.ScanFolder("dr"))
	fi, _ := m.testCurrentFolderFile("dr", "a")
	if fi.IsDrained() || fi.IsInvalid() || fi.IsDeleted() {
		t.Fatalf("a restored file should be a valid local file: %v", fi)
	}
	if fi.Version.Compare(drained.Version) != protocol.Greater {
		t.Errorf("a restored file should be a new version: %v vs %v", fi.Version, drained.Version)
	}
}

func TestDrainSkipsFilesChangedSinceScan(t *testing.T) {
	m, f := setupDrainFolder(t, nil)
	writeDrainFiles(t, m, f, map[string]int{"a": 100}, "a")
	conn := addFakeConn(m, device1, "dr")
	announce(t, m, conn, "a")

	// Change the file without letting the scanner see it.
	writeFile(t, f.Filesystem(), "a", []byte("changed, not yet scanned"))
	runPull(t, f)

	if !onDisk(t, f, "a") {
		t.Fatal("a file changed since the last scan must not be drained")
	}
	if fi, _ := m.testCurrentFolderFile("dr", "a"); fi.IsDrained() {
		t.Fatal("the entry must not be marked drained")
	}
}
