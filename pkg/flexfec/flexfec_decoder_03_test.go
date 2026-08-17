// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package flexfec

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/pion/logging"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDecoderSSRC         = uint32(1234)
	testProtectedStreamSSRC = uint32(5678)
)

func TestFECDecoderInsertPacketRemovesOldFEC(t *testing.T) {
	decoder := newFECDecoder(testDecoderSSRC, testProtectedStreamSSRC, logging.NewDefaultLoggerFactory())
	decoder.receivedFECPackets = []fecPacketState{
		newFecPacketState(1),
		newFecPacketState(500),
		newFecPacketState(1500),
		newFecPacketState(25000),
	}

	pkt := rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 40000,
			SSRC:           testDecoderSSRC,
		},
		Payload: buildTestFlexFecPayload(40000),
	}

	require.NoError(t, decoder.insertPacket(pkt))

	require.Len(t, decoder.receivedFECPackets, 2)
	assert.Equal(t, uint16(25000), decoder.receivedFECPackets[0].packet.SequenceNumber)
	assert.Equal(t, uint16(40000), decoder.receivedFECPackets[1].packet.SequenceNumber)
}

func TestFECDecoderInsertPacketKeepsRecentFEC(t *testing.T) {
	decoder := newFECDecoder(testDecoderSSRC, testProtectedStreamSSRC, logging.NewDefaultLoggerFactory())
	initialStates := []fecPacketState{
		newFecPacketState(1),
		newFecPacketState(500),
		newFecPacketState(1500),
	}
	decoder.receivedFECPackets = append(decoder.receivedFECPackets, initialStates...)

	pkt := rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: 2000,
			SSRC:           testDecoderSSRC,
		},
		Payload: buildTestFlexFecPayload(2000),
	}

	require.NoError(t, decoder.insertPacket(pkt))

	require.Len(t, decoder.receivedFECPackets, len(initialStates)+1)
	for i, state := range initialStates {
		assert.Equal(t, state.packet.SequenceNumber, decoder.receivedFECPackets[i].packet.SequenceNumber)
	}
	assert.Equal(t, uint16(2000), decoder.receivedFECPackets[len(initialStates)].packet.SequenceNumber)
}

func TestDecoder03RecoversExactPacketAfterReorderedInput(t *testing.T) {
	mediaPackets, fecPackets := generatePacketsWithFecCount(t, []uint16{1, 2, 3, 4, 5}, 1)
	decoder := NewDecoder03(ssrc, protectedStreamSSRC, logging.NewDefaultLoggerFactory())
	for _, packet := range []rtp.Packet{mediaPackets[4], fecPackets[0], mediaPackets[0], mediaPackets[1]} {
		_, err := decoder.Decode(packet)
		require.NoError(t, err)
	}
	recovered, err := decoder.Decode(mediaPackets[3])
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, mediaPackets[2], recovered[0])
}

func TestDecoder03RejectsOversizedRecoveryWithoutFalsePacket(t *testing.T) {
	mediaPackets, fecPackets := generatePacketsWithFecCount(t, []uint16{1, 2, 3, 4, 5}, 1)
	malformed := fecPackets[0].Clone()
	malformed.Payload[2] = 0xff
	malformed.Payload[3] = 0xff
	decoder := NewDecoder03(ssrc, protectedStreamSSRC, logging.NewDefaultLoggerFactory())
	for _, packet := range []rtp.Packet{mediaPackets[0], *malformed, mediaPackets[1], mediaPackets[3]} {
		_, err := decoder.Decode(packet)
		require.NoError(t, err)
	}
	recovered, err := decoder.Decode(mediaPackets[4])
	require.ErrorIs(t, err, ErrInvalidPacket)
	assert.Empty(t, recovered)
}

func TestDecoder03RejectsMalformedPacketsAndSerializesCallers(t *testing.T) {
	decoder := NewDecoder03(testDecoderSSRC, testProtectedStreamSSRC, logging.NewDefaultLoggerFactory())
	recovered, err := decoder.Decode(rtp.Packet{Header: rtp.Header{SSRC: testDecoderSSRC}, Payload: []byte{1, 2, 3}})
	require.ErrorIs(t, err, ErrInvalidPacket)
	assert.Empty(t, recovered)
	var workers sync.WaitGroup
	errorsCh := make(chan error, 8)
	for worker := range 8 {
		workers.Add(1)
		go func(offset int) {
			defer workers.Done()
			for index := offset; index < 1000; index += 8 {
				sequenceNumber := uint16(index) //nolint:gosec // The loop is bounded below the uint16 limit.
				_, decodeErr := decoder.Decode(rtp.Packet{
					Header: rtp.Header{SSRC: testProtectedStreamSSRC, SequenceNumber: sequenceNumber},
				})
				if decodeErr != nil && !errors.Is(decodeErr, ErrInvalidPacket) {
					errorsCh <- decodeErr

					return
				}
			}
		}(worker)
	}
	workers.Wait()
	close(errorsCh)
	for decodeErr := range errorsCh {
		require.NoError(t, decodeErr)
	}
}

func newFecPacketState(seq uint16) fecPacketState {
	return fecPacketState{
		packet: rtp.Packet{
			Header: rtp.Header{
				SequenceNumber: seq,
				SSRC:           testDecoderSSRC,
			},
		},
	}
}

func buildTestFlexFecPayload(seqNumBase uint16) []byte {
	payload := make([]byte, BaseFec03HeaderSize+4)
	payload[8] = 1
	binary.BigEndian.PutUint32(payload[12:], testProtectedStreamSSRC)
	binary.BigEndian.PutUint16(payload[16:], seqNumBase)
	binary.BigEndian.PutUint16(payload[18:], 0x8001)

	return payload
}
