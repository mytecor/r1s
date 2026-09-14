package runtime

import "fmt"

// Resources is an allocator-owned profile, independent of a concrete runtime.
// All-zero is the legacy unbounded profile; configured profiles must set all limits.
type Resources struct {
	MemoryBytes int64 `json:"memory_bytes"`
	CPUMilli    int64 `json:"cpu_milli"`
	Pids        int64 `json:"pids"`
}

func (r Resources) Validate() error {
	if r == (Resources{}) {
		return nil
	}
	if r.MemoryBytes < 1<<20 || r.CPUMilli <= 0 || r.CPUMilli > 1_000_000 || r.Pids <= 0 || r.Pids > 1_000_000 {
		return fmt.Errorf("invalid resource profile: all limits must be positive and bounded")
	}
	return nil
}
