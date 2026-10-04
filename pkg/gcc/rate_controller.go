// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math"
	"sync"
	"time"
)

const (
	decreaseEMAAlpha        = 0.95
	beta                    = 0.85
	minimumDecreaseInterval = 200 * time.Millisecond
)

type rateController struct {
	now                  now
	initialTargetBitrate int
	minBitrate           int
	maxBitrate           int

	dsWriter func(DelayStats)

	lock               sync.Mutex
	init               bool
	delayStats         DelayStats
	target             int
	lastUpdate         time.Time
	lastState          state
	latestRTT          time.Duration
	latestReceivedRate int
	latestDecreaseRate *exponentialMovingAverage
	lastDecrease       time.Time
	recoveryTarget     int
}

type exponentialMovingAverage struct {
	average      float64
	variance     float64
	stdDeviation float64
}

func (a *exponentialMovingAverage) update(value float64) {
	if a.average == 0.0 {
		a.average = value
	} else {
		x := value - a.average
		a.average += decreaseEMAAlpha * x
		a.variance = (1 - decreaseEMAAlpha) * (a.variance + decreaseEMAAlpha*x*x)
		a.stdDeviation = math.Sqrt(a.variance)
	}
}

func newRateController(
	now now, initialTargetBitrate, minBitrate, maxBitrate int, dsw func(DelayStats),
) *rateController {
	return &rateController{
		now:                  now,
		initialTargetBitrate: initialTargetBitrate,
		minBitrate:           minBitrate,
		maxBitrate:           maxBitrate,
		dsWriter:             dsw,
		init:                 false,
		delayStats:           DelayStats{},
		target:               initialTargetBitrate,
		lastUpdate:           time.Time{},
		lastState:            stateIncrease,
		latestRTT:            0,
		latestReceivedRate:   0,
		latestDecreaseRate:   &exponentialMovingAverage{},
	}
}

func (c *rateController) onReceivedRate(rate int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.latestReceivedRate = rate
}

// Snapshot diagnostics without introducing another controller or a logging
// operation in the feedback path. Bitrates include the tracked RTP headers and payload.
func (c *rateController) rateStats() (acknowledged, recovery int, increaseMode string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	increaseMode = "multiplicative"
	if c.recoveryTarget > c.target {
		increaseMode = "recovery"
	} else if c.latestDecreaseRate.average > 0 &&
		float64(c.latestReceivedRate) > c.latestDecreaseRate.average-3*c.latestDecreaseRate.stdDeviation &&
		float64(c.latestReceivedRate) < c.latestDecreaseRate.average+3*c.latestDecreaseRate.stdDeviation {
		increaseMode = "additive"
	}

	return c.latestReceivedRate, c.recoveryTarget, increaseMode
}

// limitBitrateIncrease also bounds recovery of a loss-limited target. The
// delay controller can retain a higher estimate while the source sends less;
// that old estimate alone is not evidence for increasing the combined target.
func (c *rateController) limitBitrateIncrease(previous, proposed int) int {
	if proposed <= previous {
		return proposed
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	return max(previous, min(proposed, int(1.5*float64(c.latestReceivedRate))))
}

func (c *rateController) updateRTT(rtt time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.latestRTT = rtt
}

func (c *rateController) onDelayStats(ds DelayStats) {
	now := c.now()

	if !c.init {
		c.delayStats = ds
		c.delayStats.State = stateIncrease
		c.init = true

		return
	}
	previousState := c.delayStats.State
	c.delayStats = ds
	c.delayStats.State = previousState.transition(ds.Usage)

	if c.delayStats.State == stateHold {
		return
	}

	var next DelayStats

	c.lock.Lock()

	switch c.delayStats.State {
	case stateHold:
		// should never occur due to check above, but makes the linter happy
	case stateIncrease:
		c.target = clampInt(c.increase(now), c.minBitrate, c.maxBitrate)
		next = DelayStats{
			Measurement:      c.delayStats.Measurement,
			Estimate:         c.delayStats.Estimate,
			Threshold:        c.delayStats.Threshold,
			LastReceiveDelta: c.delayStats.LastReceiveDelta,
			Usage:            c.delayStats.Usage,
			State:            c.delayStats.State,
			TargetBitrate:    c.target,
		}

	case stateDecrease:
		previousTarget := c.target
		if c.lastDecrease.IsZero() || now.Sub(c.lastDecrease) >= minimumDecreaseInterval {
			c.target = clampInt(c.decrease(now), c.minBitrate, c.maxBitrate)
			c.lastDecrease = now
		}
		if c.target < previousTarget {
			c.recoveryTarget = max(c.recoveryTarget, previousTarget)
		}
		next = DelayStats{
			Measurement:      c.delayStats.Measurement,
			Estimate:         c.delayStats.Estimate,
			Threshold:        c.delayStats.Threshold,
			LastReceiveDelta: c.delayStats.LastReceiveDelta,
			Usage:            c.delayStats.Usage,
			State:            c.delayStats.State,
			TargetBitrate:    c.target,
		}
	}

	c.lock.Unlock()

	c.dsWriter(next)
}

func (c *rateController) increase(now time.Time) int {
	if c.recoveryTarget > c.target {
		rate := min(c.multiplicativeIncrease(now), c.recoveryTarget)
		if rate >= c.recoveryTarget {
			c.recoveryTarget = 0
		}

		return rate
	}
	if c.latestDecreaseRate.average > 0 &&
		float64(c.latestReceivedRate) > c.latestDecreaseRate.average-3*c.latestDecreaseRate.stdDeviation &&
		float64(c.latestReceivedRate) < c.latestDecreaseRate.average+3*c.latestDecreaseRate.stdDeviation {
		bitsPerFrame := float64(c.target) / 30.0
		packetsPerFrame := math.Ceil(bitsPerFrame / (1200 * 8))
		expectedPacketSizeBits := bitsPerFrame / packetsPerFrame

		responseTime := 100*time.Millisecond + c.latestRTT
		alpha := 0.5 * math.Min(float64(now.Sub(c.lastUpdate).Milliseconds())/float64(responseTime.Milliseconds()), 1.0)
		increase := int(math.Max(1000.0, alpha*expectedPacketSizeBits))
		c.lastUpdate = now

		rate := int(math.Min(float64(c.target+increase), 1.5*float64(c.latestReceivedRate)))
		if rate < c.target {
			return c.target
		}

		return rate
	}

	return c.multiplicativeIncrease(now)
}

func (c *rateController) multiplicativeIncrease(now time.Time) int {
	eta := math.Pow(1.08, math.Min(float64(now.Sub(c.lastUpdate).Milliseconds())/1000, 1.0))
	c.lastUpdate = now

	rate := int(eta * float64(c.target))

	// maximum increase to 1.5 * received rate
	received := int(1.5 * float64(c.latestReceivedRate))
	// A source can remain below the granted rate (for example while a loss
	// hold is active). Clean feedback then proves only that lower throughput,
	// not that capacity has recovered. Preserve the current estimate instead
	// of increasing beyond the receive-rate bound or reducing it in increase
	// state. This also applies to recovery toward a pre-congestion target.
	return max(c.target, min(rate, received))
}

func (c *rateController) decrease(now time.Time) int {
	// Back off below delivered throughput to drain self-induced delay, as in
	// Pion's original AIMD rule. A floor derived from the old sending target
	// would keep flooding a suddenly narrower link. Never increase on overuse.
	target := min(c.target, int(beta*float64(c.latestReceivedRate)))
	c.latestDecreaseRate.update(float64(c.latestReceivedRate))
	c.lastUpdate = now

	return target
}
