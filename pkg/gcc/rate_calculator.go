// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math"
	"sort"
	"time"

	"github.com/pion/interceptor/internal/cc"
)

// Bound storage even when a receiver reports a stopped or compressed clock.
// Only arrival and size belong in this window, not the complete ACK record.
const maximumRateWindowPackets = 32 * 1024

type rateSample struct {
	arrival time.Time
	size    int
}

type rateCalculator struct {
	window  time.Duration
	history []rateSample
	first   int
	count   int
	bytes   int64
}

func newRateCalculator(window time.Duration) *rateCalculator {
	return &rateCalculator{window: window}
}

func (c *rateCalculator) at(index int) rateSample {
	return c.history[(c.first+index)%len(c.history)]
}

func (c *rateCalculator) removeFirst() {
	c.bytes -= int64(c.history[c.first].size)
	c.first = (c.first + 1) % len(c.history)
	c.count--
}

func (c *rateCalculator) trimBefore(cutoff time.Time) {
	for c.count > 0 && c.at(0).arrival.Before(cutoff) {
		c.removeFirst()
	}
}

func (c *rateCalculator) insert(index int, sample rateSample) {
	if c.count == maximumRateWindowPackets {
		// Keep the newest bounded window if packet density exceeds its limit.
		if index == 0 {
			return
		}
		c.removeFirst()
		index--
	}
	if c.count == len(c.history) {
		history := make([]rateSample, min(max(16, 2*len(c.history)), maximumRateWindowPackets))
		for i := range c.count {
			history[i] = c.at(i)
		}
		c.history, c.first = history, 0
	}
	// Ordered arrivals append in constant time. Only reordered receipts move
	// entries; expiry advances the ring head without copying or allocating.
	for i := c.count; i > index; i-- {
		c.history[(c.first+i)%len(c.history)] = c.at(i - 1)
	}
	c.history[(c.first+index)%len(c.history)] = sample
	c.count++
	c.bytes += int64(sample.size)
}

func (c *rateCalculator) add(ack cc.Acknowledgment) (int, bool) {
	if ack.Arrival.IsZero() || ack.Size <= 0 || ack.Size > 65535 {
		return 0, false
	}
	newest := ack.Arrival
	if c.count > 0 && c.at(c.count-1).arrival.After(newest) {
		newest = c.at(c.count - 1).arrival
	}
	cutoff := newest.Add(-c.window)
	c.trimBefore(cutoff)
	if ack.Arrival.Before(cutoff) {
		return 0, false
	}
	index := c.count
	if c.count > 0 && ack.Arrival.Before(c.at(c.count-1).arrival) {
		index = sort.Search(c.count, func(i int) bool { return c.at(i).arrival.After(ack.Arrival) })
	}
	c.insert(index, rateSample{arrival: ack.Arrival, size: ack.Size})

	return c.rate()
}

func (c *rateCalculator) rate() (int, bool) {
	first := c.at(0)
	duration := c.at(c.count - 1).arrival.Sub(first.arrival)
	if duration <= 0 {
		// A single timestamp, including multiple quantized simultaneous
		// arrivals, supplies no elapsed interval and therefore no rate.
		return 0, false
	}
	// The first packet precedes the measured interval. Counting its bytes
	// overestimates steady throughput, particularly for sparse feedback.
	rate := float64(c.bytes-int64(first.size)) * 8 / duration.Seconds()
	if rate >= float64(math.MaxInt) {
		return math.MaxInt, true
	}

	return int(rate), true
}

func (c *rateCalculator) run(in <-chan []cc.Acknowledgment, onRateUpdate func(int)) {
	for acks := range in {
		for _, ack := range acks {
			if rate, ok := c.add(ack); ok {
				onRateUpdate(rate)
			}
		}
	}
}
