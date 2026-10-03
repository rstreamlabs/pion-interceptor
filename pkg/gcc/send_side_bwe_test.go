// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/twcc"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTWCCResponder is a RTPWriter that writes
// TWCC feedback to a embedded SendSideBWE instantly.
type mockTWCCResponder struct {
	bwe     *SendSideBWE
	rtpChan chan []byte
}

func (m *mockTWCCResponder) Read(out []byte, _ interceptor.Attributes) (int, interceptor.Attributes, error) {
	pkt := <-m.rtpChan
	copy(out, pkt)

	return len(pkt), nil, nil
}

func (m *mockTWCCResponder) Write(pkts []rtcp.Packet, attributes interceptor.Attributes) (int, error) {
	return 0, m.bwe.WriteRTCP(pkts, attributes)
}

// mockGCCWriteStream receives RTP packets that have been paced by
// the congestion controller.
type mockGCCWriteStream struct {
	twccResponder *mockTWCCResponder
	t             *testing.T
}

func (m *mockGCCWriteStream) Write(header *rtp.Header, payload []byte, _ interceptor.Attributes) (int, error) {
	m.t.Helper()
	pkt, err := (&rtp.Packet{Header: *header, Payload: payload}).Marshal()
	assert.NoError(m.t, err)

	m.twccResponder.rtpChan <- pkt

	return 0, err
}

func TestSendSideBWE(t *testing.T) {
	buffer := make([]byte, 1500)
	rtpPayload := make([]byte, 1460)
	streamInfo := &interceptor.StreamInfo{
		SSRC:                1,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{{URI: transportCCURI, ID: 1}},
	}

	bwe, err := NewSendSideBWE(WithLoggerFactory(logging.NewDefaultLoggerFactory()))
	require.NoError(t, err)
	require.NotNil(t, bwe)

	gccMock := &mockGCCWriteStream{
		twccResponder: &mockTWCCResponder{
			bwe,
			make(chan []byte, 500),
		},
		t: t,
	}

	twccSender, err := (&twcc.SenderInterceptorFactory{}).NewInterceptor("")
	require.NoError(t, err)
	require.NotNil(t, twccSender)

	twccInboundRTP := twccSender.BindRemoteStream(streamInfo, gccMock.twccResponder)
	twccSender.BindRTCPWriter(gccMock.twccResponder)

	require.Equal(t, latestBitrate, bwe.GetTargetBitrate())
	require.NotEqual(t, 0, len(bwe.GetStats()))

	rtpWriter := bwe.AddStream(streamInfo, gccMock)
	require.NotNil(t, rtpWriter)

	twccWriter := twcc.HeaderExtensionInterceptor{}
	rtpWriter = twccWriter.BindLocalStream(streamInfo, rtpWriter)

	for i := 0; i <= 100; i++ {
		_, err = rtpWriter.Write(&rtp.Header{SSRC: 1, Extensions: []rtp.Extension{}}, rtpPayload, nil)
		assert.NoError(t, err)
		_, _, err = twccInboundRTP.Read(buffer, nil)
		assert.NoError(t, err)
	}

	// Sending a stream with zero loss and no RTT should increase estimate
	require.Less(t, latestBitrate, bwe.GetTargetBitrate())
}

func TestSendSideBWE_ErrorOnWriteRTCPAtClosedState(t *testing.T) {
	bwe, err := NewSendSideBWE()
	require.NoError(t, err)
	require.NotNil(t, bwe)

	pkts := []rtcp.Packet{&rtcp.TransportLayerCC{}}
	require.NoError(t, bwe.WriteRTCP(pkts, nil))
	require.Equal(t, bwe.isClosed(), false)
	require.NoError(t, bwe.Close())
	require.ErrorIs(t, bwe.WriteRTCP(pkts, nil), ErrSendSideBWEClosed)
	require.Equal(t, bwe.isClosed(), true)
}

func TestSendSideBWEAppliesConfiguredBoundsToLossController(t *testing.T) {
	const (
		initialBitrate = 8_000_000
		minimumBitrate = 2_000_000
		maximumBitrate = 10_000_000
	)
	bwe, err := NewSendSideBWE(
		SendSideBWEInitialBitrate(initialBitrate),
		SendSideBWEMinBitrate(minimumBitrate),
		SendSideBWEMaxBitrate(maximumBitrate),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bwe.Close()) })
	bwe.lossController.lock.Lock()
	bwe.lossController.bitrate = 100_000
	bwe.lossController.lock.Unlock()
	bwe.onDelayUpdate(DelayStats{TargetBitrate: initialBitrate})
	assert.Equal(t, minimumBitrate, bwe.GetTargetBitrate())
	assert.Equal(t, minimumBitrate, bwe.GetStats()["lossTargetBitrate"])
}

func TestSendSideBWELossRecoveryRequiresObservedThroughput(t *testing.T) {
	bwe, err := NewSendSideBWE(
		SendSideBWEInitialBitrate(8_000_000),
		SendSideBWEMinBitrate(2_000_000),
		SendSideBWEMaxBitrate(8_000_000),
		SendSideBWEPacer(NewNoOpPacer()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bwe.Close()) })

	applyLossEstimate := func(target int) {
		bwe.lossController.lock.Lock()
		bwe.lossController.bitrate = target
		bwe.lossController.lock.Unlock()
		bwe.onDelayUpdate(DelayStats{TargetBitrate: 8_000_000})
	}

	// The independent delay estimate can stay high after a loss reduction.
	// A source held at 2 Mbps must not ramp the effective estimate to 8 Mbps
	// merely because its reports no longer contain packet loss.
	bwe.delayController.onReceivedRate(2_000_000)
	applyLossEstimate(2_000_000)
	require.Equal(t, 2_000_000, bwe.GetTargetBitrate())
	for _, target := range []int{2_500_000, 3_000_000, 4_000_000, 8_000_000} {
		applyLossEstimate(target)
		require.Equal(t, min(target, 3_000_000), bwe.GetTargetBitrate())
	}

	// More observed traffic permits recovery; a fresh reduction still applies
	// immediately even when the received-rate bound would allow a higher rate.
	bwe.delayController.onReceivedRate(3_000_000)
	applyLossEstimate(8_000_000)
	require.Equal(t, 4_500_000, bwe.GetTargetBitrate())
	applyLossEstimate(2_500_000)
	require.Equal(t, 2_500_000, bwe.GetTargetBitrate())
	bwe.delayController.onReceivedRate(6_000_000)
	applyLossEstimate(8_000_000)
	require.Equal(t, 8_000_000, bwe.GetTargetBitrate())
}

func BenchmarkSendSideBWE_WriteRTCP(b *testing.B) {
	numSequencesPerTwccReport := []int{10, 100, 500, 1000}

	for _, count := range numSequencesPerTwccReport {
		b.Run(fmt.Sprintf("num_sequences=%d", count), func(b *testing.B) {
			bwe, err := NewSendSideBWE(SendSideBWEPacer(NewNoOpPacer()))
			require.NoError(b, err)
			require.NotNil(b, bwe)

			recorder := twcc.NewRecorder(5000)
			seq := uint16(0)
			arrivalTime := int64(0)

			for i := 0; i < b.N; i++ {
				// nolint:gosec
				seqs := rand.Intn(count/2) + count // [count, count * 1.5)
				for range seqs {
					seq++

					if rand.Intn(5) != 0 { //nolint:gosec
						arrivalTime += int64(rtcp.TypeTCCDeltaScaleFactor * (rand.Intn(128) + 1)) //nolint:gosec
						recorder.Record(5000, seq, arrivalTime)
					}
				}

				rtcpPackets := recorder.BuildFeedbackPacket()
				require.Equal(b, 1, len(rtcpPackets))

				require.NoError(b, bwe.WriteRTCP(rtcpPackets, nil))
			}

			require.NoError(b, bwe.Close())
		})
	}
}
