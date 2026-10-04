// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math"
	"sync"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
)

const (
	// constants from
	// https://datatracker.ietf.org/doc/html/draft-ietf-rmcat-gcc-02#section-6

	increaseLossThreshold = 0.02
	increaseTimeThreshold = 200 * time.Millisecond
	increaseFactor        = 1.05

	decreaseLossThreshold = 0.1
	decreaseTimeThreshold = 200 * time.Millisecond

	// Reconcile overlapping feedback and late receipts within an observation,
	// following the 250 ms send-time observation used by libwebrtc LossBasedBweV2.
	// This delays loss statistics only; no media packet is buffered.
	lossObservationDuration = 250 * time.Millisecond
	lossObservationCapacity = 1 << 15
)

// LossStats contains internal statistics of the loss based controller.
type LossStats struct {
	TargetBitrate    int
	AverageLoss      float64
	LastObservedLoss float64
	Observations     uint64
	Reductions       uint64
	Recoveries       uint64
}

type lossPacketKey struct {
	ssrc      uint32
	sequence  uint16
	departure time.Time
}

type lossBasedBandwidthEstimator struct {
	lock                 sync.Mutex
	maxBitrate           int
	minBitrate           int
	bitrate              int
	averageLoss          float64
	lastObservedLoss     float64
	observations         uint64
	reductions           uint64
	recoveries           uint64
	lastLossUpdate       time.Time
	lastIncrease         time.Time
	lastDecrease         time.Time
	log                  logging.LeveledLogger
	now                  func() time.Time
	observationPackets   int
	observationFirstSend time.Time
	observationLastSend  time.Time
	observationLost      map[lossPacketKey]struct{}
}

func newLossBasedBWE(
	initialBitrate int,
	minBitrate int,
	maxBitrate int,
	loggerFactory logging.LoggerFactory,
) *lossBasedBandwidthEstimator {
	return &lossBasedBandwidthEstimator{
		lock:           sync.Mutex{},
		maxBitrate:     maxBitrate,
		minBitrate:     minBitrate,
		bitrate:        clampInt(initialBitrate, minBitrate, maxBitrate),
		averageLoss:    0,
		lastLossUpdate: time.Time{},
		lastIncrease:   time.Time{},
		lastDecrease:   time.Time{},
		log:            loggerFactory.NewLogger("gcc_loss_controller"),
		now:            time.Now,
	}
}

func (e *lossBasedBandwidthEstimator) getEstimate(wantedRate int) LossStats {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.bitrate <= 0 {
		e.bitrate = clampInt(wantedRate, e.minBitrate, e.maxBitrate)
	}
	e.bitrate = clampInt(min(wantedRate, e.bitrate), e.minBitrate, e.maxBitrate)

	return LossStats{
		TargetBitrate:    e.bitrate,
		AverageLoss:      e.averageLoss,
		LastObservedLoss: e.lastObservedLoss,
		Observations:     e.observations,
		Reductions:       e.reductions,
		Recoveries:       e.recoveries,
	}
}

// updateLossEstimate returns whether a completed observation updated the loss
// estimate. The feedback adapter retires received packets and marks previously
// missing packets, so each transport packet contributes to the denominator once.
func (e *lossBasedBandwidthEstimator) updateLossEstimate(results []cc.Acknowledgment) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	updated := false
	for _, ack := range results {
		key := lossPacketKey{ssrc: ack.SSRC, sequence: ack.SequenceNumber, departure: ack.Departure}
		if ack.PreviouslyReportedLost {
			if !ack.Arrival.IsZero() {
				delete(e.observationLost, key)
			}

			continue
		}
		// A count bound protects even pathological feedback with identical send
		// timestamps. Complete the current observation rather than discard loss.
		if e.observationPackets == lossObservationCapacity {
			e.completeObservation()
			updated = true
		}
		e.recordNewObservation(ack, key)
	}
	if e.observationPackets > 0 &&
		e.observationLastSend.Sub(e.observationFirstSend) >= lossObservationDuration {
		e.completeObservation()
		updated = true
	}

	return updated
}

// recordNewObservation is called with e.lock held for a first report only.
func (e *lossBasedBandwidthEstimator) recordNewObservation(ack cc.Acknowledgment, key lossPacketKey) {
	if e.observationPackets == 0 {
		e.observationFirstSend = ack.Departure
		e.observationLastSend = ack.Departure
	}
	e.observationPackets++
	if ack.Departure.Before(e.observationFirstSend) {
		e.observationFirstSend = ack.Departure
	}
	if ack.Departure.After(e.observationLastSend) {
		e.observationLastSend = ack.Departure
	}
	if ack.Arrival.IsZero() {
		if e.observationLost == nil {
			e.observationLost = make(map[lossPacketKey]struct{})
		}
		e.observationLost[key] = struct{}{}
	}
}

// completeObservation is called with e.lock held. A receipt after a completed
// observation still informs delay/rate estimation, but cannot be counted again
// as a new packet in a later loss observation.
func (e *lossBasedBandwidthEstimator) completeObservation() {
	lossRatio := float64(len(e.observationLost)) / float64(e.observationPackets)
	e.lastObservedLoss = lossRatio
	e.observations++
	previousBitrate := e.bitrate
	now := e.now()
	e.averageLoss = e.average(now.Sub(e.lastLossUpdate), e.averageLoss, lossRatio)
	e.lastLossUpdate = now
	e.observationPackets = 0
	clear(e.observationLost)

	increaseLoss := math.Max(e.averageLoss, lossRatio)
	decreaseLoss := math.Min(e.averageLoss, lossRatio)

	if increaseLoss < increaseLossThreshold && now.Sub(e.lastIncrease) > increaseTimeThreshold {
		e.log.Infof(
			"loss controller increasing; averageLoss: %v, decreaseLoss: %v, increaseLoss: %v",
			e.averageLoss, decreaseLoss, increaseLoss,
		)
		e.lastIncrease = now
		e.bitrate = clampInt(int(increaseFactor*float64(e.bitrate)), e.minBitrate, e.maxBitrate)
		if e.bitrate > previousBitrate {
			e.recoveries++
		}
	} else if decreaseLoss > decreaseLossThreshold && now.Sub(e.lastDecrease) > decreaseTimeThreshold {
		e.log.Infof(
			"loss controller decreasing; averageLoss: %v, decreaseLoss: %v, increaseLoss: %v",
			e.averageLoss, decreaseLoss, increaseLoss,
		)
		e.lastDecrease = now
		e.bitrate = clampInt(int(float64(e.bitrate)*(1-0.5*decreaseLoss)), e.minBitrate, e.maxBitrate)
		if e.bitrate < previousBitrate {
			e.reductions++
		}
	}
}

func (e *lossBasedBandwidthEstimator) average(delta time.Duration, prev, sample float64) float64 {
	return sample + math.Exp(-float64(delta.Milliseconds())/200.0)*(prev-sample)
}
