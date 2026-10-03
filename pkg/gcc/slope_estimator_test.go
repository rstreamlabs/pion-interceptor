// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/stretchr/testify/assert"
)

func TestSlopeEstimatorVariableGroupDurations(t *testing.T) {
	// Constant propagation delay must not look like a growing network queue
	// when the sender emits bursts with different durations.
	start := time.Unix(100, 0)
	var measurements []time.Duration
	slope := newSlopeEstimator(estimatorFunc(identity), func(ds DelayStats) {
		measurements = append(measurements, ds.Measurement)
	})
	for _, offsets := range [][]int{{0, 2}, {10, 14}, {20, 21}, {30, 35}} {
		var group arrivalGroup
		for i, offset := range offsets {
			sent := start.Add(time.Duration(offset) * time.Millisecond)
			ack := cc.Acknowledgment{Departure: sent, Arrival: sent.Add(10 * time.Millisecond)}
			if i == 0 {
				group = newArrivalGroup(ack)
			} else {
				group.add(ack)
			}
		}
		slope.onArrivalGroup(group)
	}
	assert.Equal(t, []time.Duration{0, 0, 0}, measurements)
}

func identity(d time.Duration) time.Duration {
	return d
}

func TestSlopeEstimator(t *testing.T) {
	cases := []struct {
		name     string
		ags      []arrivalGroup
		expected []DelayStats
	}{
		{
			name:     "emptyReturnsEmpty",
			ags:      []arrivalGroup{},
			expected: []DelayStats{},
		},
		{
			name: "simpleDeltaTest",
			ags: []arrivalGroup{
				{
					arrival:         time.Time{}.Add(5 * time.Millisecond),
					departure:       time.Time{}.Add(15 * time.Millisecond),
					latestDeparture: time.Time{}.Add(15 * time.Millisecond),
				},
				{
					arrival:         time.Time{}.Add(10 * time.Millisecond),
					departure:       time.Time{}.Add(20 * time.Millisecond),
					latestDeparture: time.Time{}.Add(20 * time.Millisecond),
				},
			},
			expected: []DelayStats{
				{
					Measurement:      0,
					Estimate:         0,
					Threshold:        0,
					LastReceiveDelta: 5 * time.Millisecond,
					Usage:            0,
					State:            0,
					TargetBitrate:    0,
				},
			},
		},
		{
			name: "twoMeasurements",
			ags: []arrivalGroup{
				{
					arrival:         time.Time{}.Add(5 * time.Millisecond),
					departure:       time.Time{}.Add(15 * time.Millisecond),
					latestDeparture: time.Time{}.Add(15 * time.Millisecond),
				},
				{
					arrival:         time.Time{}.Add(10 * time.Millisecond),
					departure:       time.Time{}.Add(20 * time.Millisecond),
					latestDeparture: time.Time{}.Add(20 * time.Millisecond),
				},
				{
					arrival:         time.Time{}.Add(15 * time.Millisecond),
					departure:       time.Time{}.Add(30 * time.Millisecond),
					latestDeparture: time.Time{}.Add(30 * time.Millisecond),
				},
			},
			expected: []DelayStats{
				{
					Measurement:      0,
					Estimate:         0,
					Threshold:        0,
					LastReceiveDelta: 5 * time.Millisecond,
					Usage:            0,
					State:            0,
					TargetBitrate:    0,
				},
				{
					Measurement:      -5 * time.Millisecond,
					Estimate:         -5 * time.Millisecond,
					Threshold:        0,
					LastReceiveDelta: 5 * time.Millisecond,
					Usage:            0,
					State:            0,
					TargetBitrate:    0,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := make(chan DelayStats)
			se := newSlopeEstimator(estimatorFunc(identity), func(ds DelayStats) {
				out <- ds
			})
			input := []time.Duration{}
			go func() {
				defer close(out)
				for _, ag := range tc.ags {
					se.onArrivalGroup(ag)
				}
			}()
			received := []DelayStats{}
			for d := range out {
				received = append(received, d)
			}
			assert.Equal(t, tc.expected, received, "%v != %v", input, received)
		})
	}
}
