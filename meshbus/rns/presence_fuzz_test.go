package rns

import "testing"

func FuzzParsePresence(f *testing.F) {
	f.Add([]byte(`{"p":"meshbus.v1","realm":"0000000000000000000000000000000000000000000000000000000000000000","meta":{"k":"v"}}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = parsePresence(data)
	})
}
