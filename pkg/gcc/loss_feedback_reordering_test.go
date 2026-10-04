// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lossFeedbackFlight(t *testing.T, count int) (*cc.FeedbackAdapter, time.Time) {
	t.Helper()
	adapter := cc.NewFeedbackAdapter()
	start := time.Unix(100, 0)
	for sequence := range count {
		header := &rtp.Header{SSRC: 42, SequenceNumber: uint16(sequence)}                            //nolint:gosec // G115
		extension, err := (&rtp.TransportCCExtension{TransportSequence: uint16(sequence)}).Marshal() //nolint:gosec // G115
		require.NoError(t, err)
		require.NoError(t, header.SetExtension(1, extension))
		require.NoError(t, adapter.OnSent(start.Add(time.Duration(sequence)*time.Millisecond), header, 1000,
			interceptor.Attributes{cc.TwccExtensionAttributesKey: uint8(1)}))
	}

	return adapter, start
}

func lossFeedback(t *testing.T, adapter *cc.FeedbackAdapter, start time.Time,
	base, count int, missing func(int) bool,
) []cc.Acknowledgment {
	t.Helper()
	symbols := make([]uint16, count)
	deltas := make([]*rtcp.RecvDelta, 0, count)
	for offset := range count {
		if missing(base + offset) {
			continue
		}
		symbols[offset] = rtcp.TypeTCCPacketReceivedSmallDelta
		deltas = append(deltas, &rtcp.RecvDelta{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000})
	}
	// Split into legal one-bit vectors; leave the final in-memory vector unpadded.
	chunks := make([]rtcp.PacketStatusChunk, 0, (count+13)/14)
	for offset := 0; offset < count; offset += 14 {
		chunks = append(chunks, &rtcp.StatusVectorChunk{
			Type:       rtcp.TypeTCCStatusVectorChunk,
			SymbolSize: rtcp.TypeTCCSymbolSizeOneBit, SymbolList: symbols[offset:min(offset+14, count)],
		})
	}
	acks, err := adapter.OnTransportCCFeedback(start.Add(time.Second), &rtcp.TransportLayerCC{
		BaseSequenceNumber: uint16(base), PacketStatusCount: uint16(count), ReferenceTime: 10, //nolint:gosec // G115
		PacketChunks: chunks, RecvDeltas: deltas,
	})
	require.NoError(t, err)

	return acks
}

func TestLossFeedbackReconcilesReorderingAndRepeatedMissingReports(t *testing.T) {
	adapter, start := lossFeedbackFlight(t, 300)
	estimator := newLossBasedBWE(8_000_000, 2_000_000, 10_000_000, logging.NewDefaultLoggerFactory())
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 0, 100, func(sequence int) bool {
		return sequence == 1 || sequence >= 20 && sequence < 45
	}))
	assert.Equal(t, 8_000_000, estimator.getEstimate(8_000_000).TargetBitrate,
		"an incomplete short observation must not treat reordered packets as persistent loss")
	// A later, overlapping report receives the delayed packets. The one genuinely
	// missing packet appears eight times; it must still count exactly once.
	for range 8 {
		acks := lossFeedback(t, adapter, start, 0, 100, func(sequence int) bool { return sequence == 1 })
		estimator.updateLossEstimate(acks)
	}
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 100, 200, func(int) bool { return false }))
	stats := estimator.getEstimate(8_000_000)
	assert.InDelta(t, 1.0/300, stats.AverageLoss, 0.000001)
	assert.Equal(t, 8_000_000, stats.TargetBitrate)
}

func TestLossFeedbackStillReducesForPersistentLoss(t *testing.T) {
	adapter, start := lossFeedbackFlight(t, 300)
	estimator := newLossBasedBWE(8_000_000, 2_000_000, 10_000_000, logging.NewDefaultLoggerFactory())
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 0, 300, func(sequence int) bool { return sequence < 90 }))
	stats := estimator.getEstimate(8_000_000)
	assert.InDelta(t, 0.3, stats.AverageLoss, 0.000001)
	assert.Equal(t, 6_800_000, stats.TargetBitrate)
	assert.Equal(t, uint64(1), stats.Observations)
	assert.Equal(t, uint64(1), stats.Reductions)
	assert.Equal(t, uint64(0), stats.Recoveries)
}

func TestLossFeedbackDoesNotCountLateReceiptInNextObservation(t *testing.T) {
	adapter, start := lossFeedbackFlight(t, 600)
	estimator := newLossBasedBWE(8_000_000, 2_000_000, 10_000_000, logging.NewDefaultLoggerFactory())
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 0, 300, func(sequence int) bool { return sequence == 1 }))
	assert.InDelta(t, 1.0/300, estimator.averageLoss, 0.000001)
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 0, 300, func(int) bool { return false }))
	assert.Zero(t, estimator.observationPackets, "a receipt from a closed observation is not a new sent packet")
	assert.Empty(t, estimator.observationLost)
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 300, 100, func(int) bool { return false }))
	assert.Equal(t, 100, estimator.observationPackets)
}

func TestLossObservationHasHardMemoryBound(t *testing.T) {
	estimator := newLossBasedBWE(8_000_000, 2_000_000, 10_000_000, logging.NewDefaultLoggerFactory())
	for sequence := range 3*lossObservationCapacity + 1 {
		estimator.updateLossEstimate([]cc.Acknowledgment{{SSRC: uint32(sequence / 65536), //nolint:gosec // G115
			SequenceNumber: uint16(sequence % 65536), Departure: time.Unix(100, 0)}}) //nolint:gosec // G115
		require.LessOrEqual(t, estimator.observationPackets, lossObservationCapacity)
		require.LessOrEqual(t, len(estimator.observationLost), lossObservationCapacity)
	}
	assert.Equal(t, 1, estimator.observationPackets)
	assert.Equal(t, 1.0, estimator.averageLoss)
}

func TestSendSideBWEPublishesLossWithoutDelayMeasurement(t *testing.T) {
	pacer := &lossTargetPacer{}
	estimator, err := NewSendSideBWE(SendSideBWEInitialBitrate(8_000_000), SendSideBWEMinBitrate(2_000_000),
		SendSideBWEMaxBitrate(8_000_000), SendSideBWEPacer(pacer))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, estimator.Close()) })
	adapter, _ := lossFeedbackFlight(t, 300)
	estimator.feedbackAdapter = adapter
	callbacks := make(chan int, 1)
	estimator.OnTargetBitrateChange(func(bitrate int) { callbacks <- bitrate })
	require.NoError(t, estimator.WriteRTCP([]rtcp.Packet{&rtcp.TransportLayerCC{
		BaseSequenceNumber: 0, PacketStatusCount: 300,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketNotReceived, RunLength: 300,
		}},
	}}, nil))
	assert.Equal(t, 4_000_000, estimator.GetTargetBitrate())
	assert.Equal(t, 1.0, estimator.GetStats()["averageLoss"])
	assert.Equal(t, true, estimator.GetStats()["lossLimited"])
	assert.Equal(t, uint64(1), estimator.GetStats()["lossReductions"])
	pacer.mu.Lock()
	assert.Equal(t, 4_000_000, pacer.target)
	pacer.mu.Unlock()
	select {
	case target := <-callbacks:
		assert.Equal(t, 4_000_000, target)
	case <-time.After(time.Second):
		assert.Fail(t, "loss-only feedback did not publish its target")
	}
}

type lossTargetPacer struct {
	roundTripTimePacer
	target int
}

func (p *lossTargetPacer) SetTargetBitrate(target int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
}

func TestLossFeedbackRecoversFromResolvedLoss(t *testing.T) {
	adapter, start := lossFeedbackFlight(t, 2400)
	estimator := newLossBasedBWE(8_000_000, 2_000_000, 8_000_000, logging.NewDefaultLoggerFactory())
	now := start.Add(time.Second)
	estimator.now = func() time.Time { return now }
	estimator.updateLossEstimate(lossFeedback(t, adapter, start, 0, 300, func(sequence int) bool { return sequence < 90 }))
	assert.Equal(t, 6_800_000, estimator.getEstimate(8_000_000).TargetBitrate)
	for base := 300; base < 2400; base += 300 {
		now = now.Add(300 * time.Millisecond)
		estimator.updateLossEstimate(lossFeedback(t, adapter, start, base, 300, func(int) bool { return false }))
	}
	stats := estimator.getEstimate(8_000_000)
	assert.Greater(t, stats.TargetBitrate, 6_800_000)
	assert.LessOrEqual(t, stats.TargetBitrate, 8_000_000)
	assert.Less(t, stats.AverageLoss, 0.02)
	assert.False(t, estimator.updateLossEstimate(nil), "no feedback cannot authorize further recovery")
}
