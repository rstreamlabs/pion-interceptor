// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateCalculator(t *testing.T) {
	t0 := time.Now()
	cases := []struct {
		name     string
		acks     []cc.Acknowledgment
		expected []int
	}{
		{
			name:     "emptyCreatesNoRate",
			acks:     []cc.Acknowledgment{},
			expected: []int{},
		},
		{
			name: "ignoresZeroArrivalTimes",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           0,
				Departure:      time.Time{},
				Arrival:        time.Time{},
			}},
			expected: []int{},
		},
		{
			name: "singleAckHasNoMeasuredDuration",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           1000,
				Departure:      time.Time{},
				Arrival:        t0,
			}},
			expected: []int{},
		},
		{
			name: "twoAcksCalculateCorrectRates",
			acks: []cc.Acknowledgment{{
				SequenceNumber: 0,
				Size:           125,
				Departure:      time.Time{},
				Arrival:        t0,
			}, {
				SequenceNumber: 0,
				Size:           125,
				Departure:      time.Time{},
				Arrival:        t0.Add(100 * time.Millisecond),
			}},
			expected: []int{10_000},
		},
		{
			name: "steadyACKsCalculateCorrectRates",
			acks: getACKStream(10, 1200, 100*time.Millisecond),
			expected: []int{
				96_000,
				96_000,
				96_000,
				96_000,
				96_000,
				96_000,
				96_000,
				96_000,
				96_000,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc := newRateCalculator(500 * time.Millisecond)
			in := make(chan []cc.Acknowledgment)
			out := make(chan int)
			onRateUpdate := func(rate int) {
				out <- rate
			}
			go func() {
				defer close(out)
				rc.run(in, onRateUpdate)
			}()
			go func() {
				in <- tc.acks
				close(in)
			}()

			received := []int{}
			for r := range out {
				received = append(received, r)
			}
			assert.Equal(t, tc.expected, received)
		})
	}
}

func getACKStream(length int, size int, interval time.Duration) []cc.Acknowledgment {
	res := []cc.Acknowledgment{}
	t0 := time.Now()
	for range length {
		res = append(res, cc.Acknowledgment{
			Size:    size,
			Arrival: t0,
		})
		t0 = t0.Add(interval)
	}

	return res
}

func TestRateCalculator_ConstantArrival(t *testing.T) {
	start := time.Unix(1, 0)
	acks := make([]cc.Acknowledgment, 10000)
	for i := range acks {
		acks[i] = cc.Acknowledgment{Arrival: start, Size: 100}
	}
	last := []cc.Acknowledgment{{Arrival: start.Add(100 * time.Millisecond), Size: 100}}
	var rate int
	allocations := testing.AllocsPerRun(1, func() {
		in := make(chan []cc.Acknowledgment, 2)
		in <- acks
		in <- last
		close(in)
		newRateCalculator(time.Second).run(in, func(next int) {
			rate = next
		})
	})
	assert.LessOrEqual(t, allocations, float64(16))
	assert.Equal(t, 80_000_000, rate)
}

func TestRateCalculator_History(t *testing.T) {
	start := time.Unix(1, 0)
	history := newRateCalculator(500 * time.Millisecond)
	for i := range 2 * maximumRateWindowPackets {
		history.add(cc.Acknowledgment{Arrival: start.Add(time.Duration(i) * time.Microsecond), Size: 100})
	}
	assert.Equal(t, maximumRateWindowPackets, history.count)
	assert.Equal(t, int64(maximumRateWindowPackets*100), history.bytes)
	assert.Equal(t, start.Add(maximumRateWindowPackets*time.Microsecond), history.at(0).arrival)
	before, _ := history.rate()
	after, _ := history.add(cc.Acknowledgment{Arrival: start, Size: 100})
	assert.Equal(t, before, after)
	assert.Equal(t, maximumRateWindowPackets, history.count)
	for i := 1; i < history.count; i++ {
		assert.False(t, history.at(i).arrival.Before(history.at(i-1).arrival))
	}
	history.add(cc.Acknowledgment{Arrival: start.Add(time.Second), Size: 100})
	assert.Equal(t, 1, history.count)
	assert.Equal(t, int64(100), history.bytes)
}

func TestRateCalculator_TWCCArrivalOrder(t *testing.T) {
	for _, test := range []struct {
		name       string
		references []uint32
		deltas     [][]int64
		expected   []int
	}{
		{
			name:       "negative delta",
			references: []uint32{1},
			deltas:     [][]int64{{0, 2000, -1000, 2000}},
			expected:   []int{400000, 800000, 800000},
		},
		{
			name:       "reordered batches",
			references: []uint32{1, 1},
			deltas:     [][]int64{{0, 2000}, {1000, 2000}},
			expected:   []int{400000, 800000, 800000},
		},
		{
			name:       "reference rollover",
			references: []uint32{0xffffff, 0},
			deltas:     [][]int64{{1000, 1000}, {1000, 1000}},
			expected:   []int{800000, 25000, 36923},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := make(chan []cc.Acknowledgment, len(test.references))
			adapter := cc.NewFeedbackAdapter()
			for i, reference := range test.references {
				in <- rateTestTWCCAcks(t, adapter, reference, test.deltas[i])
			}
			close(in)
			var rates []int
			newRateCalculator(500*time.Millisecond).run(in, func(rate int) {
				rates = append(rates, rate)
			})
			assert.Equal(t, test.expected, rates)
		})
	}
}

func rateTestTWCCAcks(t *testing.T, adapter *cc.FeedbackAdapter, reference uint32, deltas []int64) []cc.Acknowledgment {
	t.Helper()

	attributes := interceptor.Attributes{}
	attributes.Set(cc.TwccExtensionAttributesKey, uint8(1))
	feedback := &rtcp.TransportLayerCC{ReferenceTime: reference}
	for i, delta := range deltas {
		header := &rtp.Header{}
		extension := rtp.TransportCCExtension{TransportSequence: uint16(i)} //nolint:gosec // test inputs contain at most four packets
		data, err := extension.Marshal()
		require.NoError(t, err)
		require.NoError(t, header.SetExtension(1, data))
		require.NoError(t, adapter.OnSent(time.Unix(1, 0), header, 100-header.MarshalSize(), attributes))
		feedback.RecvDeltas = append(feedback.RecvDeltas, &rtcp.RecvDelta{
			Type: rtcp.TypeTCCPacketReceivedLargeDelta, Delta: delta,
		})
		feedback.PacketStatusCount++
	}
	feedback.PacketChunks = []rtcp.PacketStatusChunk{&rtcp.RunLengthChunk{
		PacketStatusSymbol: rtcp.TypeTCCPacketReceivedLargeDelta,
		RunLength:          feedback.PacketStatusCount,
	}}
	acks, err := adapter.OnTransportCCFeedback(time.Unix(1, 0), feedback)
	require.NoError(t, err)

	return acks
}
