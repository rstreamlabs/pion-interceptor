// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package cc

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransportFeedbackRetainsDelayedFlight(t *testing.T) {
	adapter := NewFeedbackAdapter()
	start := time.Unix(100, 0)
	attributes := interceptor.Attributes{TwccExtensionAttributesKey: hdrExtID}
	deltas := make([]*rtcp.RecvDelta, 1000)
	// 500-byte payloads at 2000 packets/s represent an 8 Mbit/s source. A
	// half-second flight must still be observable when delayed feedback arrives.
	for sequence := range uint16(1000) {
		packet := getPacketWithTransportCCExt(t, sequence)
		require.NoError(t, adapter.OnSent(start.Add(time.Duration(sequence)*500*time.Microsecond),
			&packet.Header, 500, attributes))
		deltas[sequence] = &rtcp.RecvDelta{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 500}
	}
	acks, err := adapter.OnTransportCCFeedback(start.Add(time.Second), &rtcp.TransportLayerCC{
		BaseSequenceNumber: 0, PacketStatusCount: 1000, ReferenceTime: 10,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 1000,
		}},
		RecvDeltas: deltas,
	})
	require.NoError(t, err)
	require.Equal(t, 1000, len(acks), "the flight must not be evicted before feedback")
	assert.Equal(t, uint16(0), acks[0].SequenceNumber)
	assert.Equal(t, uint16(999), acks[999].SequenceNumber)
	assert.Empty(t, adapter.history.items, "received packets release their history immediately")
}

func TestTransportFeedbackReleasesReceivedAndRetainsMissingPackets(t *testing.T) {
	adapter := NewFeedbackAdapter()
	start := time.Unix(100, 0)
	attributes := interceptor.Attributes{TwccExtensionAttributesKey: hdrExtID}
	for sequence := uint16(1); sequence <= 3; sequence++ {
		packet := getPacketWithTransportCCExt(t, sequence)
		require.NoError(t, adapter.OnSent(start, &packet.Header, 500, attributes))
	}
	feedback := &rtcp.TransportLayerCC{
		BaseSequenceNumber: 1, PacketStatusCount: 3, ReferenceTime: 10,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.StatusVectorChunk{
			Type: rtcp.TypeTCCStatusVectorChunk, SymbolSize: rtcp.TypeTCCSymbolSizeOneBit,
			SymbolList: []uint16{1, 0, 1},
		}},
		RecvDeltas: []*rtcp.RecvDelta{
			{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000},
			{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 500},
		},
	}
	acks, err := adapter.OnTransportCCFeedback(start.Add(time.Second), feedback)
	require.NoError(t, err)
	require.Len(t, acks, 3)
	assert.True(t, acks[1].Arrival.IsZero())
	assert.Len(t, adapter.history.items, 1)
	_, missingRetained := adapter.history.get(feedbackHistoryKey{sequenceNumber: 2})
	assert.True(t, missingRetained)

	feedback.PacketChunks = []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
		Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 3,
	}}
	feedback.RecvDeltas = append(feedback.RecvDeltas, &rtcp.RecvDelta{
		Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 500,
	})
	acks, err = adapter.OnTransportCCFeedback(start.Add(2*time.Second), feedback)
	require.NoError(t, err)
	require.Len(t, acks, 1, "overlapping received feedback must not count the same packet again")
	assert.Equal(t, uint16(2), acks[0].SequenceNumber)
	assert.False(t, acks[0].Arrival.IsZero())
	assert.Empty(t, adapter.history.items)
}

func TestFeedbackHistoryExpiresOldMissingPackets(t *testing.T) {
	history := newFeedbackHistory(5)
	start := time.Unix(100, 0)
	history.add(Acknowledgment{SequenceNumber: 1, Departure: start})
	history.add(Acknowledgment{SequenceNumber: 2, Departure: start.Add(59 * time.Second)})
	history.add(Acknowledgment{SequenceNumber: 3, Departure: start.Add(61 * time.Second)})
	_, oldPresent := history.get(feedbackHistoryKey{sequenceNumber: 1})
	_, recentPresent := history.get(feedbackHistoryKey{sequenceNumber: 2})
	assert.False(t, oldPresent, "unresolved history must expire")
	assert.True(t, recentPresent)
}

func TestFeedbackHistoryHasAHardCountBound(t *testing.T) {
	adapter := NewFeedbackAdapter()
	start := time.Unix(100, 0)
	for sequence := range uint16(32868) {
		adapter.history.add(Acknowledgment{SequenceNumber: sequence, Departure: start})
	}
	assert.Equal(t, 32768, len(adapter.history.items))
	assert.Equal(t, len(adapter.history.items), adapter.history.evictList.Len())
	_, oldestPresent := adapter.history.get(feedbackHistoryKey{sequenceNumber: 99})
	_, boundaryPresent := adapter.history.get(feedbackHistoryKey{sequenceNumber: 100})
	assert.False(t, oldestPresent)
	assert.True(t, boundaryPresent)
}

func TestInvalidFeedbackDoesNotDiscardSendHistory(t *testing.T) {
	adapter := NewFeedbackAdapter()
	start := time.Unix(100, 0)
	for sequence := uint16(1); sequence <= 2; sequence++ {
		packet := getPacketWithTransportCCExt(t, sequence)
		require.NoError(t, adapter.OnSent(start, &packet.Header, 500,
			interceptor.Attributes{TwccExtensionAttributesKey: hdrExtID}))
	}
	feedback := &rtcp.TransportLayerCC{
		BaseSequenceNumber: 1, PacketStatusCount: 2, ReferenceTime: 10,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 2,
		}},
		RecvDeltas: []*rtcp.RecvDelta{{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000}},
	}
	_, err := adapter.OnTransportCCFeedback(start.Add(time.Second), feedback)
	require.ErrorIs(t, err, errInvalidFeedback)
	assert.Len(t, adapter.history.items, 2)
	feedback.RecvDeltas = append(feedback.RecvDeltas, &rtcp.RecvDelta{
		Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 500,
	})
	acks, err := adapter.OnTransportCCFeedback(start.Add(time.Second), feedback)
	require.NoError(t, err)
	assert.Len(t, acks, 2)
	assert.Empty(t, adapter.history.items)
}

func TestRFC8888FeedbackReleasesReceivedHistory(t *testing.T) {
	adapter := NewFeedbackAdapter()
	start := time.Unix(100, 0)
	for sequence := uint16(1); sequence <= 2; sequence++ {
		header := &rtp.Header{SSRC: 42, SequenceNumber: sequence}
		require.NoError(t, adapter.OnSent(start, header, 500, nil))
	}
	feedback := &rtcp.CCFeedbackReport{
		ReportTimestamp: 1,
		ReportBlocks: []rtcp.CCFeedbackReportBlock{{
			MediaSSRC: 42, BeginSequence: 1,
			MetricBlocks: []rtcp.CCFeedbackMetricBlock{{Received: true}, {Received: false}},
		}},
	}
	acks := adapter.OnRFC8888Feedback(start.Add(time.Second), feedback)
	assert.Len(t, acks, 2)
	assert.Len(t, adapter.history.items, 1)
	feedback.ReportBlocks[0].MetricBlocks[1].Received = true
	acks = adapter.OnRFC8888Feedback(start.Add(2*time.Second), feedback)
	require.Len(t, acks, 1)
	assert.Equal(t, uint16(2), acks[0].SequenceNumber)
	assert.Empty(t, adapter.history.items)
}

func BenchmarkTransportFeedbackFlight(b *testing.B) {
	adapter := NewFeedbackAdapter()
	attributes := interceptor.Attributes{TwccExtensionAttributesKey: hdrExtID}
	header := &rtp.Header{Version: 2}
	require.NoError(b, header.SetExtension(hdrExtID, []byte{0, 0}))
	deltas := make([]*rtcp.RecvDelta, 64)
	for index := range deltas {
		deltas[index] = &rtcp.RecvDelta{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 500}
	}
	feedback := &rtcp.TransportLayerCC{
		PacketStatusCount: 64, ReferenceTime: 10,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 64,
		}},
		RecvDeltas: deltas,
	}
	start := time.Unix(100, 0)
	var sequence uint16
	b.ReportAllocs()
	b.ResetTimer()
	for batch := 0; batch < b.N; batch++ {
		feedback.BaseSequenceNumber = sequence
		for range 64 {
			binary.BigEndian.PutUint16(header.GetExtension(hdrExtID), sequence)
			if err := adapter.OnSent(start, header, 500, attributes); err != nil {
				b.Fatal(err)
			}
			sequence++
		}
		if _, err := adapter.OnTransportCCFeedback(start.Add(time.Second), feedback); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*64), "ns/packet")
}

func TestFeedbackTracksMissingReportsUntilLateReceipt(t *testing.T) {
	for _, format := range []string{"twcc", "rfc8888"} {
		t.Run(format, func(t *testing.T) {
			adapter := NewFeedbackAdapter()
			start := time.Unix(100, 0)
			ssrc := uint32(0)
			if format == "rfc8888" {
				ssrc = 42
			}
			adapter.history.add(Acknowledgment{SSRC: ssrc, SequenceNumber: 1, Departure: start})
			report := func(received bool) []Acknowledgment {
				t.Helper()
				if format == "rfc8888" {
					return adapter.OnRFC8888Feedback(start.Add(time.Second), &rtcp.CCFeedbackReport{
						ReportTimestamp: 65536,
						ReportBlocks: []rtcp.CCFeedbackReportBlock{{
							MediaSSRC: ssrc, BeginSequence: 1,
							MetricBlocks: []rtcp.CCFeedbackMetricBlock{{Received: received}},
						}},
					})
				}
				symbol := rtcp.TypeTCCPacketNotReceived
				var deltas []*rtcp.RecvDelta
				if received {
					symbol = rtcp.TypeTCCPacketReceivedSmallDelta
					deltas = []*rtcp.RecvDelta{{Type: symbol, Delta: 1000}}
				}
				acks, err := adapter.OnTransportCCFeedback(start.Add(time.Second), &rtcp.TransportLayerCC{
					BaseSequenceNumber: 1, PacketStatusCount: 1, ReferenceTime: 10,
					PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
						Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: symbol, RunLength: 1,
					}}, RecvDeltas: deltas,
				})
				require.NoError(t, err)

				return acks
			}
			first := report(false)
			require.Len(t, first, 1)
			assert.False(t, first[0].PreviouslyReportedLost)
			repeat := report(false)
			require.Len(t, repeat, 1)
			assert.True(t, repeat[0].PreviouslyReportedLost)
			late := report(true)
			require.Len(t, late, 1)
			assert.True(t, late[0].PreviouslyReportedLost)
			assert.False(t, late[0].Arrival.IsZero())
			assert.Empty(t, report(true), "received duplicates must not re-enter estimation")
		})
	}
}

func TestMalformedFeedbackDoesNotMarkEarlierPacketsLost(t *testing.T) {
	adapter := NewFeedbackAdapter()
	for sequence := uint16(1); sequence <= 2; sequence++ {
		adapter.history.add(Acknowledgment{SequenceNumber: sequence, Departure: time.Unix(100, 0)})
	}
	_, err := adapter.OnTransportCCFeedback(time.Unix(101, 0), &rtcp.TransportLayerCC{
		BaseSequenceNumber: 1, PacketStatusCount: 2, ReferenceTime: 10,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.StatusVectorChunk{
			Type: rtcp.TypeTCCStatusVectorChunk, SymbolSize: rtcp.TypeTCCSymbolSizeOneBit, SymbolList: []uint16{0, 1},
		}},
	})
	require.Error(t, err)
	ack, present := adapter.history.get(feedbackHistoryKey{sequenceNumber: 1})
	require.True(t, present)
	assert.False(t, ack.PreviouslyReportedLost)
}
