// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/stretchr/testify/require"
)

func calculateTestRates(acks ...cc.Acknowledgment) []int {
	in := make(chan []cc.Acknowledgment, 1)
	in <- acks
	close(in)
	var rates []int
	newRateCalculator(500*time.Millisecond).run(in, func(rate int) { rates = append(rates, rate) })

	return rates
}

func TestRateCalculatorEqualArrivalTimesNeverProduceInvalidRate(t *testing.T) {
	start := time.Unix(100, 0)
	rates := calculateTestRates(
		cc.Acknowledgment{Arrival: start, Size: 1000},
		cc.Acknowledgment{Arrival: start, Size: 1000},
		cc.Acknowledgment{Arrival: start.Add(100 * time.Millisecond), Size: 1000},
	)
	for _, rate := range rates {
		require.Positive(t, rate, "quantized simultaneous arrivals have no elapsed time to divide by")
		require.LessOrEqual(t, rate, 160_000)
	}
	// There are 2000 bytes after the first packet over a 100 ms interval.
	require.Equal(t, 160_000, rates[len(rates)-1])
}

func TestRateCalculatorUsesChronologicalWindowForReorderedReceipts(t *testing.T) {
	start := time.Unix(100, 0)
	rates := calculateTestRates(
		cc.Acknowledgment{Arrival: start, Size: 1000},
		cc.Acknowledgment{Arrival: start.Add(100 * time.Millisecond), Size: 1000},
		cc.Acknowledgment{Arrival: start.Add(50 * time.Millisecond), Size: 1000},
	)
	require.Equal(t, 160_000, rates[len(rates)-1], "late feedback must not move the window endpoint backward")
}

func TestRateCalculatorDoesNotCountFirstPacketOutsideMeasuredInterval(t *testing.T) {
	start := time.Unix(100, 0)
	rates := calculateTestRates(
		cc.Acknowledgment{Arrival: start, Size: 2000},
		cc.Acknowledgment{Arrival: start.Add(100 * time.Millisecond), Size: 1000},
	)
	require.Equal(t, 80_000, rates[len(rates)-1])
}

func TestRateCalculatorExpiredAndInvalidReceiptsCannotMoveWindowBackward(t *testing.T) {
	start := time.Unix(100, 0)
	calculator := newRateCalculator(500 * time.Millisecond)
	for _, offset := range []time.Duration{0, 100 * time.Millisecond, time.Second, 1100 * time.Millisecond} {
		calculator.add(cc.Acknowledgment{Arrival: start.Add(offset), Size: 1000})
	}
	for _, ack := range []cc.Acknowledgment{
		{Arrival: start, Size: 1000},
		{Arrival: start.Add(2 * time.Second), Size: -1},
		{Arrival: start.Add(2 * time.Second), Size: 65536},
		{Size: 1000},
	} {
		_, valid := calculator.add(ack)
		require.False(t, valid)
	}
	rate, valid := calculator.add(cc.Acknowledgment{Arrival: start.Add(1200 * time.Millisecond), Size: 1000})
	require.True(t, valid)
	require.Equal(t, 80_000, rate)
	require.Equal(t, 3, calculator.count)
}

func TestRateCalculatorBoundsStoppedClockAndIntegerRange(t *testing.T) {
	start := time.Unix(100, 0)
	calculator := newRateCalculator(500 * time.Millisecond)
	for range maximumRateWindowPackets * 3 {
		_, valid := calculator.add(cc.Acknowledgment{Arrival: start, Size: 65535})
		require.False(t, valid)
	}
	require.Equal(t, maximumRateWindowPackets, calculator.count)
	require.Equal(t, maximumRateWindowPackets, len(calculator.history))
	rate, valid := calculator.add(cc.Acknowledgment{Arrival: start.Add(time.Nanosecond), Size: 65535})
	require.True(t, valid)
	require.Positive(t, rate)
	require.LessOrEqual(t, rate, math.MaxInt)
	require.Equal(t, int64(maximumRateWindowPackets)*65535, calculator.bytes)
}

func TestRateCalculatorMatchesChronologicalReferenceAcrossRingWraps(t *testing.T) {
	calculator := newRateCalculator(500 * time.Millisecond)
	random := rand.New(rand.NewSource(1)) //nolint:gosec // Reproducible timing permutations.
	start := time.Unix(100, 0)
	var receipts []cc.Acknowledgment
	var newest time.Time
	for index := range 5000 {
		ack := cc.Acknowledgment{
			Arrival: start.Add(time.Duration(index*2+random.Intn(400)) * time.Millisecond),
			Size:    100 + random.Intn(1100),
		}
		rate, valid := calculator.add(ack)
		if ack.Arrival.After(newest) {
			newest = ack.Arrival
		}
		cutoff := newest.Add(-500 * time.Millisecond)
		if ack.Arrival.Before(cutoff) {
			require.False(t, valid)

			continue
		}
		receipts = append(receipts, ack)
		sort.SliceStable(receipts, func(i, j int) bool { return receipts[i].Arrival.Before(receipts[j].Arrival) })
		for len(receipts) > 0 && receipts[0].Arrival.Before(cutoff) {
			receipts = receipts[1:]
		}
		duration := newest.Sub(receipts[0].Arrival)
		if duration == 0 {
			require.False(t, valid)

			continue
		}
		var bytes int64
		for _, receipt := range receipts[1:] {
			bytes += int64(receipt.Size)
		}
		require.True(t, valid)
		require.Equal(t, int(float64(bytes)*8/duration.Seconds()), rate, "receipt %d", index)
	}
}

func TestRateCalculatorOrderedSteadyStateDoesNotAllocate(t *testing.T) {
	calculator := newRateCalculator(500 * time.Millisecond)
	arrival := time.Unix(100, 0)
	add := func() {
		arrival = arrival.Add(time.Millisecond)
		calculator.add(cc.Acknowledgment{Arrival: arrival, Size: 1200})
	}
	for range 2000 {
		add()
	}
	require.Zero(t, testing.AllocsPerRun(1000, add))
}

func TestRateCalculatorUsesConfiguredWindow(t *testing.T) {
	for _, scenario := range []struct {
		window   time.Duration
		expected int
	}{
		{100 * time.Millisecond, 160_000},
		{500 * time.Millisecond, 120_000},
	} {
		calculator := newRateCalculator(scenario.window)
		start := time.Unix(100, 0)
		calculator.add(cc.Acknowledgment{Arrival: start, Size: 1000})
		calculator.add(cc.Acknowledgment{Arrival: start.Add(100 * time.Millisecond), Size: 1000})
		rate, valid := calculator.add(cc.Acknowledgment{Arrival: start.Add(200 * time.Millisecond), Size: 2000})
		require.True(t, valid)
		require.Equal(t, scenario.expected, rate)
	}
}

func BenchmarkRateCalculatorOrdered(b *testing.B) {
	calculator := newRateCalculator(500 * time.Millisecond)
	arrival := time.Unix(100, 0)
	b.ReportAllocs()
	for range b.N {
		arrival = arrival.Add(time.Millisecond)
		calculator.add(cc.Acknowledgment{Arrival: arrival, Size: 1200})
	}
}
