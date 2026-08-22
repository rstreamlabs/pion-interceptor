// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package rtpbuffer

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/pion/rtp"
)

const rtxSsrcByteLength = 2

// PacketFactory allows custom logic around the handle of RTP Packets before they added to the RTPBuffer.
// The NoOpPacketFactory doesn't copy packets, while the RetainablePacket will take a copy before adding.
type PacketFactory interface {
	NewPacket(header *rtp.Header, payload []byte, rtxSsrc uint32, rtxPayloadType uint8) (*RetainablePacket, error)
	// PrepareRetransmission consumes the retained packet reference returned by
	// RTPBuffer.Get and returns the packet that may be written downstream.
	PrepareRetransmission(packet *RetainablePacket, sequencer rtp.Sequencer) (*RetainablePacket, error)
}

// PacketFactoryCopy is PacketFactory that takes a copy of packets when added to the RTPBuffer.
type PacketFactoryCopy struct {
	headerPool  *sync.Pool
	payloadPool *sync.Pool
}

// NewPacketFactoryCopy constructs a PacketFactory that takes a copy of packets when added to the RTPBuffer.
func NewPacketFactoryCopy() *PacketFactoryCopy {
	return &PacketFactoryCopy{
		headerPool: &sync.Pool{
			New: func() any {
				return &rtp.Header{}
			},
		},
		payloadPool: &sync.Pool{
			New: func() any {
				buf := make([]byte, maxPayloadLen)

				return &buf
			},
		},
	}
}

// NewPacket constructs a new RetainablePacket that can be added to the RTPBuffer.
//
//nolint:cyclop
func (m *PacketFactoryCopy) NewPacket(
	header *rtp.Header, payload []byte, rtxSsrc uint32, rtxPayloadType uint8,
) (*RetainablePacket, error) {
	isRTX := rtxSsrc != 0 && rtxPayloadType != 0
	requiredPayloadLength := len(payload)
	if isRTX {
		requiredPayloadLength += rtxSsrcByteLength
	}
	if requiredPayloadLength > maxPayloadLen {
		return nil, io.ErrShortBuffer
	}

	retainablePacket := &RetainablePacket{
		onRelease:      m.releasePacket,
		retransmission: isRTX,
		sequenceNumber: header.SequenceNumber,
		// new packets have retain count of 1
		count: 1,
	}

	var ok bool
	retainablePacket.header, ok = m.headerPool.Get().(*rtp.Header)
	if !ok {
		return nil, errFailedToCastHeaderPool
	}

	*retainablePacket.header = header.Clone()

	if payload != nil {
		retainablePacket.buffer, ok = m.payloadPool.Get().(*[]byte)
		if !ok {
			return nil, errFailedToCastPayloadPool
		}
		if isRTX {
			size := copy((*retainablePacket.buffer)[rtxSsrcByteLength:], payload)
			retainablePacket.payload = (*retainablePacket.buffer)[:size+rtxSsrcByteLength]
		} else {
			size := copy(*retainablePacket.buffer, payload)
			retainablePacket.payload = (*retainablePacket.buffer)[:size]
		}
	}

	if isRTX { //nolint:nestif
		if payload == nil {
			retainablePacket.buffer, ok = m.payloadPool.Get().(*[]byte)
			if !ok {
				return nil, errFailedToCastPayloadPool
			}
			retainablePacket.payload = (*retainablePacket.buffer)[:rtxSsrcByteLength]
		}
		// Write the original sequence number at the beginning of the payload.
		binary.BigEndian.PutUint16(retainablePacket.payload, retainablePacket.header.SequenceNumber)

		// Rewrite the SSRC.
		retainablePacket.header.SSRC = rtxSsrc
		// Rewrite the payload type.
		retainablePacket.header.PayloadType = rtxPayloadType
		// Remove padding if present.
		if retainablePacket.header.Padding {
			// Older versions of pion/rtp didn't have the Header.PaddingSize field and as a workaround
			// users had to add padding to the payload. We need to handle this case here.
			if retainablePacket.header.PaddingSize == 0 && len(retainablePacket.payload) > 0 {
				paddingLength := int(retainablePacket.payload[len(retainablePacket.payload)-1])
				if paddingLength > len(retainablePacket.payload) {
					return nil, errPaddingOverflow
				}
				retainablePacket.payload = (*retainablePacket.buffer)[:len(retainablePacket.payload)-paddingLength]
			}

			retainablePacket.header.Padding = false
			retainablePacket.header.PaddingSize = 0
		}
	}

	return retainablePacket, nil
}

// PrepareRetransmission assigns an RTX sequence number when a cached packet is
// actually retransmitted. Advancing the RTX sequence for every cached primary
// packet can make two sparse repairs appear more than half a sequence space
// apart, causing receivers to reject the later repair as stale.
func (m *PacketFactoryCopy) PrepareRetransmission(
	packet *RetainablePacket,
	sequencer rtp.Sequencer,
) (*RetainablePacket, error) {
	if packet == nil {
		return nil, errNilPacket
	}
	if !packet.retransmission {
		return packet, nil
	}
	if sequencer == nil {
		packet.Release()
		return nil, errRTXSequencerRequired
	}
	clone, err := m.clonePacket(packet)
	packet.Release()
	if err != nil {
		return nil, err
	}
	clone.header.SequenceNumber = sequencer.NextSequenceNumber()
	return clone, nil
}

func (m *PacketFactoryCopy) clonePacket(packet *RetainablePacket) (*RetainablePacket, error) {
	header, ok := m.headerPool.Get().(*rtp.Header)
	if !ok {
		return nil, errFailedToCastHeaderPool
	}
	*header = packet.header.Clone()
	clone := &RetainablePacket{
		count:          1,
		header:         header,
		onRelease:      m.releasePacket,
		retransmission: packet.retransmission,
		sequenceNumber: packet.sequenceNumber,
	}
	if packet.payload == nil {
		return clone, nil
	}
	buffer, ok := m.payloadPool.Get().(*[]byte)
	if !ok {
		m.headerPool.Put(header)
		return nil, errFailedToCastPayloadPool
	}
	payload := (*buffer)[:len(packet.payload)]
	copy(payload, packet.payload)
	clone.buffer = buffer
	clone.payload = payload
	return clone, nil
}

func (m *PacketFactoryCopy) releasePacket(header *rtp.Header, payload *[]byte) {
	m.headerPool.Put(header)
	if payload != nil {
		m.payloadPool.Put(payload)
	}
}

// PacketFactoryNoOp is a PacketFactory implementation that doesn't copy packets.
type PacketFactoryNoOp struct{}

// NewPacket constructs a new RetainablePacket that can be added to the RTPBuffer.
func (f *PacketFactoryNoOp) NewPacket(
	header *rtp.Header, payload []byte, _ uint32, _ uint8,
) (*RetainablePacket, error) {
	return &RetainablePacket{
		onRelease:      f.releasePacket,
		count:          1,
		header:         header,
		payload:        payload,
		sequenceNumber: header.SequenceNumber,
	}, nil
}

// PrepareRetransmission returns the retained source packet unchanged when
// copying and RTX encapsulation are disabled.
func (f *PacketFactoryNoOp) PrepareRetransmission(
	packet *RetainablePacket,
	_ rtp.Sequencer,
) (*RetainablePacket, error) {
	if packet == nil {
		return nil, errNilPacket
	}
	return packet, nil
}

func (f *PacketFactoryNoOp) releasePacket(_ *rtp.Header, _ *[]byte) {
	// no-op
}
