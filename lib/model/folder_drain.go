// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/semaphore"
	"github.com/syncthing/syncthing/lib/versioner"
)

func init() {
	folderFactories[config.FolderTypeDrain] = newDrainFolder
}

// drainer is implemented by folder types that remove local data once
// enough other devices hold it.
type drainer interface {
	drain(ctx context.Context) error
}

// A drainFolder is a send only folder that removes local files once they
// are held, at the same version, by at least DrainSeedLevel connected and
// trusted devices. Removed files are flagged as drained in the database:
// they keep their version, so they are neither needed (and pulled again)
// nor announced as deleted. Other devices see them as invalid and keep
// their copies.
type drainFolder struct {
	*sendOnlyFolder
	protected map[string]struct{}
}

func newDrainFolder(model *model, ignores *ignore.Matcher, cfg config.FolderConfiguration, ver versioner.Versioner, evLogger events.Logger, ioLimiter *semaphore.Semaphore) service {
	f := &drainFolder{
		sendOnlyFolder: newSendOnlyFolder(model, ignores, cfg, ver, evLogger, ioLimiter).(*sendOnlyFolder),
		protected:      protectedNames(cfg, model.protectedFiles),
	}
	f.puller = f
	return f
}

// protectedNames returns the folder relative names of the protected files
// (our own config, keys and database) that live inside the folder.
func protectedNames(cfg config.FolderConfiguration, protectedFiles []string) map[string]struct{} {
	names := make(map[string]struct{})
	ffs := cfg.Filesystem()
	if ffs.Type() != fs.FilesystemTypeBasic {
		return names
	}
	root := ffs.URI()
	for _, p := range protectedFiles {
		if !fs.IsParent(p, root) {
			continue
		}
		if rel, err := filepath.Rel(root, p); err == nil {
			names[rel] = struct{}{}
		}
	}
	return names
}

func (f *drainFolder) drain(ctx context.Context) error {
	local, err := f.db.CountLocal(f.folderID, protocol.LocalDeviceID)
	if err != nil {
		return err
	}
	high, low, err := f.drainLimits()
	if err != nil {
		f.sl.WarnContext(ctx, "Not draining, can't determine the size limits", slogutil.Error(err))
		return nil
	}
	size := local.Bytes
	if high > 0 && size <= high {
		return nil
	}
	target := min(low, high)

	seeds := f.connectedTrustedDevices()
	if len(seeds) < f.DrainSeedLevel {
		f.sl.DebugContext(ctx, "Not draining, too few connected devices", slog.Int("connected", len(seeds)), slog.Int("seedLevel", f.DrainSeedLevel))
		return nil
	}

	batch := NewFileInfoBatch(func(files []protocol.FileInfo) error {
		return f.updateLocalsFromScanning(files)
	})

	var drainedFiles int
	var drainedBytes int64
	for cand, err := range itererr.Zip(f.db.AllLocalDrainCandidates(f.folderID, f.DrainOrder)) {
		if err != nil {
			return err
		}
		if size <= target {
			break
		}
		if err := ctx.Err(); err != nil {
			break
		}
		if err := batch.FlushIfFull(); err != nil {
			return err
		}

		if _, ok := f.protected[cand.Name]; ok {
			continue
		}

		seeded, err := f.isSeeded(cand.Name, seeds)
		if err != nil {
			return err
		}
		if !seeded {
			continue
		}

		fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, cand.Name)
		if err != nil {
			return err
		}
		if !ok || fi.IsInvalid() || fi.IsDeleted() || fi.Type != protocol.FileInfoTypeFile {
			continue
		}

		if err := f.removeDrained(fi); err != nil {
			f.sl.WarnContext(ctx, "Failed to drain file", slogutil.FilePath(fi.Name), slogutil.Error(err))
			continue
		}

		fi.SetDrained()
		batch.Append(fi)
		size -= fi.Size
		drainedFiles++
		drainedBytes += fi.Size
		f.sl.DebugContext(ctx, "Drained file", slogutil.FilePath(fi.Name))
	}

	if err := batch.Flush(); err != nil {
		return err
	}
	if drainedFiles > 0 {
		f.sl.InfoContext(ctx, "Drained files replicated to other devices", slog.Int("files", drainedFiles), slog.Int64("bytes", drainedBytes))
	}
	return nil
}

// drainLimits returns the high and low water marks in bytes. A high mark of
// zero means everything sufficiently seeded is drained.
func (f *drainFolder) drainLimits() (high, low int64, err error) {
	if high, err = f.sizeBytes(f.DrainHighWater); err != nil {
		return 0, 0, err
	}
	if high <= 0 {
		return 0, 0, nil
	}
	if low, err = f.sizeBytes(f.DrainLowWater); err != nil {
		return 0, 0, err
	}
	return high, max(low, 0), nil
}

func (f *drainFolder) sizeBytes(s config.Size) (int64, error) {
	if !s.Percentage() {
		return int64(s.BaseValue()), nil
	}
	usage, err := f.mtimefs.Usage(".")
	if err != nil {
		return 0, err
	}
	return int64(float64(usage.Total) * s.BaseValue() / 100), nil
}

// connectedTrustedDevices returns the other devices sharing the folder that
// are connected right now and hold it unencrypted.
func (f *drainFolder) connectedTrustedDevices() map[protocol.DeviceID]struct{} {
	devs := make(map[protocol.DeviceID]struct{})
	for _, dev := range f.Devices {
		if dev.DeviceID == f.model.id || dev.EncryptionPassword != "" {
			continue
		}
		if f.model.ConnectedTo(dev.DeviceID) {
			devs[dev.DeviceID] = struct{}{}
		}
	}
	return devs
}

// isSeeded reports whether at least DrainSeedLevel of the given devices
// announce exactly our version of the file.
func (f *drainFolder) isSeeded(name string, seeds map[protocol.DeviceID]struct{}) (bool, error) {
	have, err := f.db.GetLocalVersionAvailability(f.folderID, name)
	if err != nil {
		return false, err
	}
	n := 0
	for _, dev := range have {
		if _, ok := seeds[dev]; ok {
			n++
		}
	}
	return n >= f.DrainSeedLevel, nil
}

// removeDrained deletes the file from disk, but only if it still matches
// what we announced. Anything else means the file changed since the last
// scan, so it's left for the scanner to pick up.
func (f *drainFolder) removeDrained(fi protocol.FileInfo) error {
	info, err := f.mtimefs.Lstat(fi.Name)
	if err != nil {
		return err
	}
	if !info.IsRegular() || info.Size() != fi.Size || !protocol.ModTimeEqual(info.ModTime(), fi.ModTime(), f.modTimeWindow) {
		f.ScheduleScan()
		return fmt.Errorf("changed on disk since last scan")
	}
	return f.mtimefs.Remove(fi.Name)
}
