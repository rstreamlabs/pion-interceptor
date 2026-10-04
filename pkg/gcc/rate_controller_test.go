// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateControllerRun(t *testing.T) {
	cases := []struct {
		name           string
		initialBitrate int
		usage          []usage
		expected       []DelayStats
	}{
		{
			name:           "empty",
			initialBitrate: 100_000,
			usage:          []usage{},
			expected:       []DelayStats{},
		},
		{
			name:           "increasesMultiplicativelyBy8000",
			initialBitrate: 100_000,
			usage:          []usage{usageNormal, usageNormal},
			expected: []DelayStats{{
				Usage:         usageNormal,
				State:         stateIncrease,
				TargetBitrate: 108_000,
				Estimate:      0,
				Threshold:     0,
			}},
		},
	}

	t0 := time.Now()
	mockNoFn := func() time.Time {
		t0 = t0.Add(100 * time.Millisecond)

		return t0
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := make(chan DelayStats)
			dc := newRateController(mockNoFn, 100_000, 1_000, 50_000_000, func(ds DelayStats) {
				out <- ds
			})
			in := make(chan DelayStats)
			dc.onReceivedRate(100_000)
			dc.updateRTT(300 * time.Millisecond)
			go func() {
				defer close(out)
				for _, state := range tc.usage {
					dc.onDelayStats(DelayStats{
						Measurement:   0,
						Estimate:      0,
						Threshold:     0,
						Usage:         state,
						State:         0,
						TargetBitrate: 0,
					})
				}
				close(in)
			}()
			received := []DelayStats{}
			for ds := range out {
				received = append(received, ds)
			}
			if len(tc.expected) > 0 {
				assert.Equal(t, tc.expected[0], received[0])
			}
		})
	}
}

func TestRateControllerReportsAcknowledgedThroughputAndIncreaseMode(t *testing.T) {
	controller := newRateController(time.Now, 2_000_000, 100_000, 8_000_000, func(DelayStats) {})
	controller.onReceivedRate(1_500_000)
	acknowledged, recovery, mode := controller.rateStats()
	assert.Equal(t, 1_500_000, acknowledged)
	assert.Zero(t, recovery)
	assert.Equal(t, "multiplicative", mode)
	controller.recoveryTarget = 6_000_000
	_, recovery, mode = controller.rateStats()
	assert.Equal(t, 6_000_000, recovery)
	assert.Equal(t, "recovery", mode)
	controller.recoveryTarget = 0
	controller.latestDecreaseRate.average = 1_500_000
	controller.latestDecreaseRate.stdDeviation = 100_000
	_, _, mode = controller.rateStats()
	assert.Equal(t, "additive", mode)
	assert.Equal(t, 2_000_000, controller.target, "reading diagnostics cannot change the target")
}

func TestRateControllerIncreaseDoesNotReduceTarget(t *testing.T) {
	now := time.Now()
	controller := newRateController(time.Now, 8_000_000, 100_000, 50_000_000, func(DelayStats) {})
	controller.target = 8_000_000
	controller.latestReceivedRate = 1_350_000
	controller.latestDecreaseRate.update(1_300_000)
	controller.latestDecreaseRate.update(1_350_000)
	controller.lastUpdate = now.Add(-100 * time.Millisecond)

	assert.GreaterOrEqual(t, controller.increase(now), controller.target)
}

func TestRateControllerDoesNotIncreaseBeyondObservedThroughput(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprintf("recovery=%v", recovery), func(t *testing.T) {
			now := time.Unix(100, 0)
			controller := newRateController(func() time.Time { return now }, 4_000_000, 100_000, 8_000_000, func(DelayStats) {})
			controller.onReceivedRate(2_000_000)
			controller.onDelayStats(DelayStats{Usage: usageNormal})
			controller.lastUpdate = now
			if recovery {
				controller.recoveryTarget = 8_000_000
			}

			// An encoder may stay below the estimate while a loss hold or a
			// source quality limit is active. Clean feedback at that lower rate
			// does not establish that the previous capacity has returned.
			for range 60 {
				now = now.Add(time.Second)
				controller.onDelayStats(DelayStats{Usage: usageNormal})
				assert.Equal(t, 4_000_000, controller.target)
			}

			// Preserve the estimate during application limitation, then allow
			// growth again as soon as received traffic supports it.
			controller.onReceivedRate(4_000_000)
			now = now.Add(time.Second)
			assert.Equal(t, 4_320_000, controller.increase(now))
		})
	}
}

func TestRateControllerIncreaseStopsAtThroughputHeadroom(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newRateController(func() time.Time { return now }, 2_900_000, 100_000, 8_000_000, func(DelayStats) {})
	controller.onReceivedRate(2_000_000)
	controller.lastUpdate = now.Add(-time.Second)

	assert.Equal(t, 3_000_000, controller.increase(now))
}

func TestRateControllerRecoversMultiplicativelyToPreDecreaseTarget(t *testing.T) {
	now := time.Now()
	controller := newRateController(func() time.Time { return now }, 8_000_000, 100_000, 50_000_000, func(DelayStats) {})
	controller.target = 8_000_000
	controller.latestReceivedRate = 2_400_000
	controller.target = controller.decrease(now)
	controller.recoveryTarget = 8_000_000
	controller.latestDecreaseRate.average = float64(controller.target)
	controller.latestDecreaseRate.stdDeviation = float64(controller.target)
	controller.onReceivedRate(controller.target)
	now = now.Add(time.Second)

	recovered := controller.increase(now)

	assert.Equal(t, 2_203_200, recovered)
	assert.Equal(t, 8_000_000, controller.recoveryTarget)
}

func TestRateControllerRecoveryStopsAtPreDecreaseTarget(t *testing.T) {
	now := time.Now()
	controller := newRateController(func() time.Time { return now }, 8_000_000, 100_000, 50_000_000, func(DelayStats) {})
	controller.target = 7_900_000
	controller.latestReceivedRate = 8_000_000
	controller.lastUpdate = now.Add(-time.Second)
	controller.recoveryTarget = 8_000_000

	recovered := controller.increase(now)

	assert.Equal(t, 8_000_000, recovered)
	assert.Zero(t, controller.recoveryTarget)
}

func TestRateControllerTracksAndRecoversADecreasedTarget(t *testing.T) {
	now := time.Now()
	updates := []DelayStats{}
	controller := newRateController(
		func() time.Time { return now },
		8_000_000,
		100_000,
		50_000_000,
		func(stats DelayStats) { updates = append(updates, stats) },
	)
	controller.onReceivedRate(2_400_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	assert.Equal(t, 2_040_000, controller.target)
	assert.Equal(t, 8_000_000, controller.recoveryTarget)
	controller.onReceivedRate(controller.target)
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageNormal})

	assert.Len(t, updates, 2)
	assert.Equal(t, 2_203_200, updates[1].TargetBitrate)
	assert.Equal(t, 8_000_000, controller.recoveryTarget)
}

func TestRateControllerBoundsRepeatedCongestionResponse(t *testing.T) {
	now := time.Now()
	controller := newRateController(
		func() time.Time { return now },
		8_000_000,
		100_000,
		8_000_000,
		func(DelayStats) {},
	)
	controller.onReceivedRate(4_000_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	for range 5 {
		now = now.Add(minimumDecreaseInterval)
		controller.onDelayStats(DelayStats{Usage: usageOver})
	}

	assert.Equal(t, 3_400_000, controller.target)
	assert.Equal(t, 8_000_000, controller.recoveryTarget)
}

func TestRateControllerRateLimitsSustainedCongestionResponse(t *testing.T) {
	now := time.Now()
	controller := newRateController(
		func() time.Time { return now },
		8_000_000,
		100_000,
		8_000_000,
		func(DelayStats) {},
	)
	controller.onReceivedRate(4_000_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(minimumDecreaseInterval)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	first := controller.target
	controller.onReceivedRate(2_000_000)
	now = now.Add(minimumDecreaseInterval / 2)
	controller.onDelayStats(DelayStats{Usage: usageOver})

	assert.Equal(t, first, controller.target)
	now = now.Add(minimumDecreaseInterval / 2)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	assert.Less(t, controller.target, first)
}

func TestRateControllerRespondsToSeparateCongestionEpisodes(t *testing.T) {
	now := time.Now()
	controller := newRateController(
		func() time.Time { return now },
		8_000_000,
		100_000,
		8_000_000,
		func(DelayStats) {},
	)
	controller.onReceivedRate(4_000_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	assert.Equal(t, 3_400_000, controller.target)
	controller.onReceivedRate(controller.target)
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	controller.onReceivedRate(2_000_000)
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})

	assert.Equal(t, 1_700_000, controller.target)
	assert.Equal(t, 8_000_000, controller.recoveryTarget)
}

func TestRateControllerRecoverySurvivesTransientOveruse(t *testing.T) {
	now := time.Now()
	controller := newRateController(
		func() time.Time { return now },
		8_000_000,
		100_000,
		8_000_000,
		func(DelayStats) {},
	)
	controller.onReceivedRate(2_400_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	for second := 1; second <= 60; second++ {
		now = now.Add(time.Second)
		usage := usageNormal
		if second%10 == 0 {
			usage = usageOver
		}
		controller.onReceivedRate(controller.target)
		controller.onDelayStats(DelayStats{Usage: usage})
	}

	assert.GreaterOrEqual(t, controller.target, 6_400_000)
	assert.LessOrEqual(t, controller.target, 8_000_000)
}
