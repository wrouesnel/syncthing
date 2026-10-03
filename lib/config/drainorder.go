// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

// DrainOrder is the order in which a drain folder removes local files that
// are sufficiently replicated elsewhere.
type DrainOrder int32

const (
	DrainOrderRandom        DrainOrder = 0
	DrainOrderAlphabetic    DrainOrder = 1
	DrainOrderSmallestFirst DrainOrder = 2
	DrainOrderLargestFirst  DrainOrder = 3
	DrainOrderOldestFirst   DrainOrder = 4
	DrainOrderNewestFirst   DrainOrder = 5
)

func (o DrainOrder) String() string {
	switch o {
	case DrainOrderRandom:
		return "random"
	case DrainOrderAlphabetic:
		return "alphabetic"
	case DrainOrderSmallestFirst:
		return "smallestFirst"
	case DrainOrderLargestFirst:
		return "largestFirst"
	case DrainOrderOldestFirst:
		return "oldestFirst"
	case DrainOrderNewestFirst:
		return "newestFirst"
	default:
		return "unknown"
	}
}

func (o DrainOrder) MarshalText() ([]byte, error) {
	return []byte(o.String()), nil
}

func (o *DrainOrder) UnmarshalText(bs []byte) error {
	switch string(bs) {
	case "random":
		*o = DrainOrderRandom
	case "alphabetic":
		*o = DrainOrderAlphabetic
	case "smallestFirst":
		*o = DrainOrderSmallestFirst
	case "largestFirst":
		*o = DrainOrderLargestFirst
	case "newestFirst":
		*o = DrainOrderNewestFirst
	default:
		*o = DrainOrderOldestFirst
	}
	return nil
}

func (o *DrainOrder) ParseDefault(s string) error {
	return o.UnmarshalText([]byte(s))
}
