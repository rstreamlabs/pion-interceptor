// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

type roundTripTimePacer struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (*roundTripTimePacer) AddStream(uint32, interceptor.RTPWriter) {}
func (*roundTripTimePacer) SetTargetBitrate(int)                    {}
func (*roundTripTimePacer) Write(*rtp.Header, []byte, interceptor.Attributes) (int, error) {
	return 0, nil
}
func (*roundTripTimePacer) Close() error { return nil }

func (p *roundTripTimePacer) ObserveRoundTripTime(value time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples = append(p.samples, value)
}

func (p *roundTripTimePacer) snapshot() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]time.Duration(nil), p.samples...)
}

func TestSendSideBWEProvidesTransportFeedbackRTTToPacer(t *testing.T) {
	pacer := &roundTripTimePacer{}
	bwe, err := NewSendSideBWE(SendSideBWEPacer(pacer))
	require.NoError(t, err)
	t.Cleanup(func() {
		if !bwe.isClosed() {
			require.NoError(t, bwe.Close())
		}
	})

	header := &rtp.Header{SSRC: 1, SequenceNumber: 1}
	require.NoError(t, header.SetExtension(1, []byte{0, 1}))
	attributes := interceptor.Attributes{}
	attributes.Set(cc.TwccExtensionAttributesKey, uint8(1))
	require.NoError(t, bwe.feedbackAdapter.OnSent(time.Now().Add(-80*time.Millisecond), header, 100, attributes))

	feedback := &rtcp.TransportLayerCC{
		BaseSequenceNumber: 1, PacketStatusCount: 1,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
			Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 1,
		}},
		RecvDeltas: []*rtcp.RecvDelta{{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000}},
	}
	require.NoError(t, bwe.WriteRTCP([]rtcp.Packet{feedback}, nil))
	samples := pacer.snapshot()
	require.Len(t, samples, 1)
	require.GreaterOrEqual(t, samples[0], 80*time.Millisecond)

	// Unknown and lost packets carry no usable round-trip observation.
	feedback.BaseSequenceNumber = 100
	require.NoError(t, bwe.WriteRTCP([]rtcp.Packet{feedback}, nil))
	feedback.BaseSequenceNumber = 1
	// Invalid clock/order data must not supply a negative RTT to the pacer.
	require.NoError(t, bwe.feedbackAdapter.OnSent(time.Now().Add(time.Hour), header, 100, attributes))
	require.NoError(t, bwe.WriteRTCP([]rtcp.Packet{feedback}, nil))
	feedback.PacketChunks = []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
		Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: rtcp.TypeTCCPacketNotReceived, RunLength: 1,
	}}
	feedback.RecvDeltas = nil
	require.NoError(t, bwe.WriteRTCP([]rtcp.Packet{feedback}, nil))
	require.Len(t, pacer.snapshot(), 1)
	require.NoError(t, bwe.Close())
	require.ErrorIs(t, bwe.WriteRTCP([]rtcp.Packet{feedback}, nil), ErrSendSideBWEClosed)
	require.Len(t, pacer.snapshot(), 1)
}
