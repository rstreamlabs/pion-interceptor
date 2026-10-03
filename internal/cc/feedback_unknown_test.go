// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package cc

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransportFeedbackPreservesDeltasAcrossUnknownPackets(t *testing.T) {
	for name, chunks := range map[string][]rtcp.PacketStatusChunk{
		"run length": {&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 3,
		}},
		"status vector": {&rtcp.StatusVectorChunk{
			Type: rtcp.TypeTCCStatusVectorChunk, SymbolSize: rtcp.TypeTCCSymbolSizeOneBit,
			SymbolList: []uint16{1, 1, 1},
		}},
		"multiple chunks": {
			&rtcp.RunLengthChunk{
				Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 2,
			},
			&rtcp.RunLengthChunk{
				Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 1,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := NewFeedbackAdapter()
			attributes := interceptor.Attributes{}
			attributes.Set(TwccExtensionAttributesKey, hdrExtID)
			for _, sequence := range []uint16{100, 102} {
				packet := getPacketWithTransportCCExt(t, sequence)
				require.NoError(t, adapter.OnSent(time.Unix(100, 0), &packet.Header, 10, attributes))
			}
			feedback := &rtcp.TransportLayerCC{
				BaseSequenceNumber: 100, PacketStatusCount: 3, ReferenceTime: 1,
				PacketChunks: chunks,
				RecvDeltas: []*rtcp.RecvDelta{
					{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000},
					{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 4000},
					{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 8000},
				},
			}
			acks, err := adapter.OnTransportCCFeedback(time.Unix(101, 0), feedback)
			require.NoError(t, err)
			assert.Len(t, acks, 2, "unknown history is not evidence of a lost packet")
			require.NotEmpty(t, acks)
			last := acks[len(acks)-1]
			assert.Equal(t, uint16(102), last.SequenceNumber)
			assert.Equal(t, time.Time{}.Add(77*time.Millisecond), last.Arrival,
				"every received delta contributes to later arrival times, including unknown packets")

			feedback.RecvDeltas = feedback.RecvDeltas[:2]
			_, err = adapter.OnTransportCCFeedback(time.Unix(101, 0), feedback)
			assert.ErrorIs(t, err, errInvalidFeedback, "unknown packets still consume their receive delta")
			feedback.RecvDeltas[1] = nil
			_, err = adapter.OnTransportCCFeedback(time.Unix(101, 0), feedback)
			assert.ErrorIs(t, err, errInvalidFeedback, "a missing delta must not panic")
		})
	}
}
