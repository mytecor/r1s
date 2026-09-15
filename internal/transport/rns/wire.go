package rns

import "fmt"

const (
	authMessageType     uint16 = 0x0100
	envelopeMessageType uint16 = 0x0101
	authNonceSize              = 32
	authProofSize              = 32
	authKindChallenge   byte   = 1
	authKindResponse    byte   = 2
)

type authMessage struct {
	kind  byte
	nonce []byte
	proof []byte
}

func (m *authMessage) Pack() ([]byte, error) {
	if len(m.nonce) != authNonceSize {
		return nil, fmt.Errorf("invalid authentication nonce")
	}
	switch m.kind {
	case authKindChallenge:
		if len(m.proof) != 0 {
			return nil, fmt.Errorf("challenge must not contain a proof")
		}
	case authKindResponse:
		if len(m.proof) != authProofSize {
			return nil, fmt.Errorf("invalid authentication proof")
		}
	default:
		return nil, fmt.Errorf("invalid authentication message kind")
	}
	data := make([]byte, 1, 1+authNonceSize+len(m.proof))
	data[0] = m.kind
	data = append(data, m.nonce...)
	data = append(data, m.proof...)
	return data, nil
}

func (m *authMessage) Unpack(data []byte) error {
	if len(data) != 1+authNonceSize && len(data) != 1+authNonceSize+authProofSize {
		return fmt.Errorf("invalid authentication message length")
	}
	m.kind = data[0]
	m.nonce = append(m.nonce[:0], data[1:1+authNonceSize]...)
	m.proof = append(m.proof[:0], data[1+authNonceSize:]...)
	_, err := m.Pack()
	return err
}

func (m *authMessage) GetType() uint16 { return authMessageType }

type envelopeMessage struct {
	data []byte
}

func (m *envelopeMessage) Pack() ([]byte, error) {
	return m.data, nil
}

func (m *envelopeMessage) Unpack(data []byte) error {
	m.data = append(m.data[:0], data...)
	return nil
}

func (m *envelopeMessage) GetType() uint16 {
	return envelopeMessageType
}
