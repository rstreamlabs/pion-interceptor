// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"fmt"
	"testing"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/stretchr/testify/assert"
)

func TestArrivalGroupDelayVariationPreservesLatestDeparture(t *testing.T) {
	start := time.Unix(100, 0)
	ack := func(sent, received time.Duration) cc.Acknowledgment {
		return cc.Acknowledgment{Departure: start.Add(sent), Arrival: start.Add(received)}
	}
	group := newArrivalGroup(ack(0, 10*time.Millisecond))
	group.add(ack(5*time.Millisecond, 15*time.Millisecond))
	group.add(ack(4*time.Millisecond, 16*time.Millisecond))
	assert.Equal(t, time.Duration(0), interGroupDelayVariationPkt(group, ack(6*time.Millisecond, 17*time.Millisecond)))
	assert.Equal(t, 6*time.Millisecond, interDepartureTimePkt(group, ack(6*time.Millisecond, 17*time.Millisecond)))
}

func TestArrivalGroupAccumulatorObservesContinuousCongestion(t *testing.T) {
	for _, arrivalInterval := range []time.Duration{time.Millisecond, 2 * time.Millisecond} {
		t.Run(fmt.Sprintf("arrival=%s", arrivalInterval), func(t *testing.T) {
			start := time.Unix(100, 0)
			acks := make([]cc.Acknowledgment, 600)
			for i := range acks {
				acks[i] = cc.Acknowledgment{
					Departure: start.Add(time.Duration(i) * time.Millisecond),
					Arrival:   start.Add(time.Second + time.Duration(i)*arrivalInterval),
				}
			}
			input := make(chan []cc.Acknowledgment, 1)
			input <- acks
			close(input)
			groups := 0
			measurements := 0
			slope := newSlopeEstimator(estimatorFunc(func(d time.Duration) time.Duration { return d }), func(ds DelayStats) {
				measurements++
				assert.Equal(t, 6*(arrivalInterval-time.Millisecond), ds.Measurement)
			})
			newArrivalGroupAccumulator().run(input, func(group arrivalGroup) {
				groups++
				assert.Len(t, group.packets, 6)
				slope.onArrivalGroup(group)
			})
			assert.Equal(t, 99, groups)
			assert.Equal(t, 98, measurements)
		})
	}
}

func TestArrivalGroupAccumulatorBoundsCompressedBursts(t *testing.T) {
	start := time.Unix(100, 0)
	acks := make([]cc.Acknowledgment, 600)
	for i := range acks {
		acks[i] = cc.Acknowledgment{
			Departure: start.Add(time.Duration(i) * 2 * time.Millisecond),
			Arrival:   start.Add(time.Second + time.Duration(i)*time.Millisecond),
		}
	}
	input := make(chan []cc.Acknowledgment, 1)
	input <- acks
	close(input)
	groups := 0
	newArrivalGroupAccumulator().run(input, func(group arrivalGroup) {
		groups++
		assert.LessOrEqual(t, group.arrival.Sub(group.packets[0].Arrival), 100*time.Millisecond)
	})
	assert.GreaterOrEqual(t, groups, 5)
}

func TestArrivalGroupAccumulatorIgnoresUnreceivedPackets(t *testing.T) {
	start := time.Unix(100, 0)
	acked := make([]cc.Acknowledgment, 0, 30)
	withLoss := []cc.Acknowledgment{{Departure: start.Add(-time.Millisecond)}}
	for i := range 30 {
		ack := cc.Acknowledgment{
			Departure: start.Add(time.Duration(i) * time.Millisecond),
			Arrival:   start.Add(time.Second + time.Duration(i)*time.Millisecond),
		}
		acked = append(acked, ack)
		withLoss = append(withLoss, ack, cc.Acknowledgment{Departure: ack.Departure.Add(500 * time.Microsecond)})
	}
	collect := func(acks []cc.Acknowledgment) []arrivalGroup {
		input := make(chan []cc.Acknowledgment, 1)
		input <- acks
		close(input)
		var groups []arrivalGroup
		newArrivalGroupAccumulator().run(input, func(group arrivalGroup) { groups = append(groups, group) })

		return groups
	}
	assert.Equal(t, collect(acked), collect(withLoss))
}

func TestArrivalGroupAccumulator(t *testing.T) {
	triggerNewGroupElement := cc.Acknowledgment{
		Departure: time.Time{}.Add(time.Second),
		Arrival:   time.Time{}.Add(time.Second),
	}
	cases := []struct {
		name string
		log  []cc.Acknowledgment
		exp  []arrivalGroup
	}{
		{
			name: "emptyCreatesNoGroups",
			log:  []cc.Acknowledgment{},
			exp:  []arrivalGroup{},
		},
		{
			name: "createsSingleElementGroup",
			log: []cc.Acknowledgment{
				{
					Departure: time.Time{},
					Arrival:   time.Time{}.Add(time.Millisecond),
				},
				triggerNewGroupElement,
			},
			exp: []arrivalGroup{
				{
					packets: []cc.Acknowledgment{{
						Departure: time.Time{},
						Arrival:   time.Time{}.Add(time.Millisecond),
					}},
					arrival:   time.Time{}.Add(time.Millisecond),
					departure: time.Time{},
				},
			},
		},
		{
			name: "createsTwoElementGroup",
			log: []cc.Acknowledgment{
				{
					Arrival: time.Time{}.Add(15 * time.Millisecond),
				},
				{
					Departure: time.Time{}.Add(3 * time.Millisecond),
					Arrival:   time.Time{}.Add(20 * time.Millisecond),
				},
				triggerNewGroupElement,
			},
			exp: []arrivalGroup{{
				packets: []cc.Acknowledgment{
					{
						Departure: time.Time{},
						Arrival:   time.Time{}.Add(15 * time.Millisecond),
					},
					{
						Departure: time.Time{}.Add(3 * time.Millisecond),
						Arrival:   time.Time{}.Add(20 * time.Millisecond),
					},
				},
				arrival:         time.Time{}.Add(20 * time.Millisecond),
				departure:       time.Time{},
				latestDeparture: time.Time{}.Add(3 * time.Millisecond),
			}},
		},
		{
			name: "createsTwoArrivalGroups",
			log: []cc.Acknowledgment{
				{
					Departure: time.Time{},
					Arrival:   time.Time{}.Add(15 * time.Millisecond),
				},
				{
					Departure: time.Time{}.Add(3 * time.Millisecond),
					Arrival:   time.Time{}.Add(20 * time.Millisecond),
				},
				{
					Departure: time.Time{}.Add(9 * time.Millisecond),
					Arrival:   time.Time{}.Add(30 * time.Millisecond),
				},
				triggerNewGroupElement,
			},
			exp: []arrivalGroup{
				{
					packets: []cc.Acknowledgment{
						{
							Arrival: time.Time{}.Add(15 * time.Millisecond),
						},
						{
							Departure: time.Time{}.Add(3 * time.Millisecond),
							Arrival:   time.Time{}.Add(20 * time.Millisecond),
						},
					},
					arrival:         time.Time{}.Add(20 * time.Millisecond),
					departure:       time.Time{}.Add(0 * time.Millisecond),
					latestDeparture: time.Time{}.Add(3 * time.Millisecond),
				},
				{
					packets: []cc.Acknowledgment{
						{
							Departure: time.Time{}.Add(9 * time.Millisecond),
							Arrival:   time.Time{}.Add(30 * time.Millisecond),
						},
					},
					arrival:         time.Time{}.Add(30 * time.Millisecond),
					departure:       time.Time{}.Add(9 * time.Millisecond),
					latestDeparture: time.Time{}.Add(9 * time.Millisecond),
				},
			},
		},
		{
			name: "ignoresOutOfOrderPackets",
			log: []cc.Acknowledgment{
				{
					Departure: time.Time{},
					Arrival:   time.Time{}.Add(15 * time.Millisecond),
				},
				{
					Departure: time.Time{}.Add(6 * time.Millisecond),
					Arrival:   time.Time{}.Add(34 * time.Millisecond),
				},
				{
					Departure: time.Time{}.Add(8 * time.Millisecond),
					Arrival:   time.Time{}.Add(30 * time.Millisecond),
				},
				triggerNewGroupElement,
			},
			exp: []arrivalGroup{
				{
					packets: []cc.Acknowledgment{
						{
							Departure: time.Time{},
							Arrival:   time.Time{}.Add(15 * time.Millisecond),
						},
					},
					arrival:   time.Time{}.Add(15 * time.Millisecond),
					departure: time.Time{},
				},
				{
					packets: []cc.Acknowledgment{
						{
							Departure: time.Time{}.Add(6 * time.Millisecond),
							Arrival:   time.Time{}.Add(34 * time.Millisecond),
						},
					},
					arrival:         time.Time{}.Add(34 * time.Millisecond),
					departure:       time.Time{}.Add(6 * time.Millisecond),
					latestDeparture: time.Time{}.Add(6 * time.Millisecond),
				},
			},
		},
		{
			name: "newGroupBecauseOfInterDepartureTime",
			log: []cc.Acknowledgment{
				{
					SequenceNumber: 0,
					Departure:      time.Time{},
					Arrival:        time.Time{}.Add(4 * time.Millisecond),
				},
				{
					SequenceNumber: 1,
					Departure:      time.Time{}.Add(3 * time.Millisecond),
					Arrival:        time.Time{}.Add(4 * time.Millisecond),
				},
				{
					SequenceNumber: 2,
					Departure:      time.Time{}.Add(6 * time.Millisecond),
					Arrival:        time.Time{}.Add(10 * time.Millisecond),
				},
				{
					SequenceNumber: 3,
					Departure:      time.Time{}.Add(9 * time.Millisecond),
					Arrival:        time.Time{}.Add(10 * time.Millisecond),
				},
				triggerNewGroupElement,
			},
			exp: []arrivalGroup{
				{
					packets: []cc.Acknowledgment{
						{
							SequenceNumber: 0,
							Departure:      time.Time{},
							Arrival:        time.Time{}.Add(4 * time.Millisecond),
						},
						{
							SequenceNumber: 1,
							Departure:      time.Time{}.Add(3 * time.Millisecond),
							Arrival:        time.Time{}.Add(4 * time.Millisecond),
						},
					},
					departure:       time.Time{},
					latestDeparture: time.Time{}.Add(3 * time.Millisecond),
					arrival:         time.Time{}.Add(4 * time.Millisecond),
				},
				{
					packets: []cc.Acknowledgment{
						{
							SequenceNumber: 2,
							Departure:      time.Time{}.Add(6 * time.Millisecond),
							Arrival:        time.Time{}.Add(10 * time.Millisecond),
						},
						{
							SequenceNumber: 3,
							Departure:      time.Time{}.Add(9 * time.Millisecond),
							Arrival:        time.Time{}.Add(10 * time.Millisecond),
						},
					},
					departure:       time.Time{}.Add(6 * time.Millisecond),
					latestDeparture: time.Time{}.Add(9 * time.Millisecond),
					arrival:         time.Time{}.Add(10 * time.Millisecond),
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aga := newArrivalGroupAccumulator()
			in := make(chan []cc.Acknowledgment)
			out := make(chan arrivalGroup)
			go func() {
				defer close(out)
				aga.run(in, func(ag arrivalGroup) {
					out <- ag
				})
			}()
			go func() {
				in <- tc.log
				close(in)
			}()
			received := []arrivalGroup{}
			for g := range out {
				received = append(received, g)
			}
			assert.Equal(t, tc.exp, received)
		})
	}
}
