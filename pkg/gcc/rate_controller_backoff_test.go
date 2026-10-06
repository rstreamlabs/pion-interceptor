// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateControllerBacksOffToReceivedThroughput(t *testing.T) {
	// A six-Mbit source hits a link delivering only 1.2 Mbit/s. Keeping 85%
	// of the old sending rate continues filling the bottleneck after overuse
	// is known. Repeated feedback must not be needed to reach the receive rate.
	now := time.Unix(10, 0)
	controller := newRateController(func() time.Time { return now }, 6_000_000, 100_000, 8_000_000, func(DelayStats) {})
	controller.onReceivedRate(1_200_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	assert.Equal(t, 1_020_000, controller.target)
	assert.Equal(t, 6_000_000, controller.recoveryTarget)

	// The same capacity observation is not another multiplicative cut.
	for range 5 {
		now = now.Add(minimumDecreaseInterval)
		controller.onDelayStats(DelayStats{Usage: usageOver})
		assert.Equal(t, 1_020_000, controller.target)
	}
}

func TestRateControllerOveruseDoesNotIncreaseTarget(t *testing.T) {
	now := time.Unix(10, 0)
	controller := newRateController(func() time.Time { return now }, 1_000_000, 100_000, 8_000_000, func(DelayStats) {})
	controller.onReceivedRate(6_000_000)
	controller.onDelayStats(DelayStats{Usage: usageNormal})
	now = now.Add(time.Second)
	controller.onDelayStats(DelayStats{Usage: usageOver})
	assert.Equal(t, 1_000_000, controller.target)
}
