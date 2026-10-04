package broker

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	protocolVersion = 1
	maxFrameBytes   = 8 << 20
)

type frame struct {
	Version     int             `json:"version,omitempty"`
	Type        string          `json:"type"`
	ID          uint64          `json:"id,omitempty"`
	Error       string          `json:"error,omitempty"`
	ClusterID   string          `json:"clusterId,omitempty"`
	Identity    string          `json:"identity,omitempty"`
	NetworkWait int64           `json:"networkWait,omitempty"`
	Target      string          `json:"target,omitempty"`
	Destination string          `json:"destination,omitempty"`
	Bootstrap   []string        `json:"bootstrap,omitempty"`
	Envelope    []byte          `json:"envelope,omitempty"`
	Service     json.RawMessage `json:"service,omitempty"`
}

func readFrame(reader io.Reader) (frame, error) {
	var sizeBytes [4]byte
	if _, err := io.ReadFull(reader, sizeBytes[:]); err != nil {
		return frame{}, err
	}
	size := binary.BigEndian.Uint32(sizeBytes[:])
	if size == 0 || size > maxFrameBytes {
		return frame{}, fmt.Errorf("broker: invalid frame size %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return frame{}, err
	}
	var message frame
	if err := json.Unmarshal(data, &message); err != nil {
		return frame{}, fmt.Errorf("broker: decode frame: %w", err)
	}
	if message.Type == "" {
		return frame{}, errors.New("broker: frame type is required")
	}
	return message, nil
}

func writeFrame(writer io.Writer, message frame) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("broker: encode frame: %w", err)
	}
	if len(data) == 0 || len(data) > maxFrameBytes {
		return fmt.Errorf("broker: frame size %d exceeds limit", len(data))
	}
	var sizeBytes [4]byte
	binary.BigEndian.PutUint32(sizeBytes[:], uint32(len(data)))
	if _, err := writer.Write(sizeBytes[:]); err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}
