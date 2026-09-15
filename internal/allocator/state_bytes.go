package allocator

// Byte-slice ownership helpers keep durable-state decoding from retaining
// storage buffers or aliasing authenticated identities.
func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func firstBytes(primary, fallback []byte) []byte {
	if len(primary) != 0 {
		return primary
	}
	return fallback
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
