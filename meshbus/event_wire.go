package meshbus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

var eventMagic = [4]byte{'M', 'B', 'E', 1}

const eventHeaderSize = 4 + 16 + 8 + 4 + 2 + 2 + 4

func isEventMessage(payload []byte) bool {
	return len(payload) >= len(eventMagic) && bytes.Equal(payload[:len(eventMagic)], eventMagic[:])
}

func encodeEvent(event Event, maxPayload int, maxTTL time.Duration) ([]byte, error) {
	if event.ID.isZero() || validateTopic(event.Topic) != nil || event.PublishedAt.IsZero() ||
		event.TTL < time.Millisecond || event.TTL > maxTTL || len(event.Payload) == 0 || len(event.Payload) > maxPayload ||
		len(event.ContentType) > 128 {
		return nil, ErrInvalidEvent
	}
	for _, character := range []byte(event.ContentType) {
		if character < 0x20 || character > 0x7e {
			return nil, ErrInvalidEvent
		}
	}
	ttlMillis := event.TTL.Milliseconds()
	if ttlMillis <= 0 || ttlMillis > int64(^uint32(0)) {
		return nil, ErrInvalidEvent
	}

	data := make([]byte, eventHeaderSize, eventHeaderSize+len(event.Topic)+len(event.ContentType)+len(event.Payload))
	copy(data[:4], eventMagic[:])
	copy(data[4:20], event.ID[:])
	binary.BigEndian.PutUint64(data[20:28], uint64(event.PublishedAt.UnixMilli()))
	binary.BigEndian.PutUint32(data[28:32], uint32(ttlMillis))
	binary.BigEndian.PutUint16(data[32:34], uint16(len(event.Topic)))
	binary.BigEndian.PutUint16(data[34:36], uint16(len(event.ContentType)))
	binary.BigEndian.PutUint32(data[36:40], uint32(len(event.Payload)))
	data = append(data, event.Topic...)
	data = append(data, event.ContentType...)
	data = append(data, event.Payload...)
	return data, nil
}

func decodeEvent(data []byte, maxPayload int, maxTTL time.Duration) (Event, error) {
	if !isEventMessage(data) || len(data) < eventHeaderSize {
		return Event{}, ErrInvalidEvent
	}
	var id EventID
	copy(id[:], data[4:20])
	publishedMillis := int64(binary.BigEndian.Uint64(data[20:28]))
	ttl := time.Duration(binary.BigEndian.Uint32(data[28:32])) * time.Millisecond
	topicLength64 := uint64(binary.BigEndian.Uint16(data[32:34]))
	contentTypeLength64 := uint64(binary.BigEndian.Uint16(data[34:36]))
	payloadLength64 := uint64(binary.BigEndian.Uint32(data[36:40]))
	wantLength := uint64(eventHeaderSize) + topicLength64 + contentTypeLength64 + payloadLength64
	if wantLength != uint64(len(data)) || payloadLength64 == 0 || payloadLength64 > uint64(maxPayload) {
		return Event{}, ErrInvalidEvent
	}
	topicLength := int(topicLength64)
	contentTypeLength := int(contentTypeLength64)
	offset := eventHeaderSize
	event := Event{
		ID: id, PublishedAt: time.UnixMilli(publishedMillis).UTC(), TTL: ttl,
		Topic: string(data[offset : offset+topicLength]),
	}
	offset += topicLength
	event.ContentType = string(data[offset : offset+contentTypeLength])
	offset += contentTypeLength
	event.Payload = bytes.Clone(data[offset:])
	if _, err := encodeEvent(event, maxPayload, maxTTL); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	return event, nil
}
