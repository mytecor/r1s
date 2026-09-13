package rns

const envelopeMessageType uint16 = 0x0101

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
