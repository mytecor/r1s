package main

import "testing"

func TestParseCapacity(t *testing.T) {
	capacity, err := parseCapacity("default=2, gpu = 1")
	if err != nil {
		t.Fatal(err)
	}
	if capacity["default"] != 2 || capacity["gpu"] != 1 {
		t.Fatalf("capacity = %v", capacity)
	}
	for _, value := range []string{"", "default", "default=0", "default=x", "default=1,default=2"} {
		if _, err := parseCapacity(value); err == nil {
			t.Errorf("parseCapacity(%q) succeeded", value)
		}
	}
}
