package allocator

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

var ErrAdmission = errors.New("client admission denied")

type Quota struct {
	Offers     uint32 `json:"offers"`
	Executions uint32 `json:"executions"`
}

type AdmissionPolicy struct {
	Profiles map[string]r1sruntime.Resources `json:"profiles"`
	// Nil means a trusted network allowing all authenticated clients. [] denies all.
	AllowedClients []string         `json:"allowed_clients"`
	DefaultQuota   Quota            `json:"default_quota"`
	ClientQuotas   map[string]Quota `json:"client_quotas"`
}

func ReadAdmission(reader io.Reader, capacity map[string]uint32) (AdmissionPolicy, error) {
	p := AdmissionPolicy{Profiles: make(map[string]r1sruntime.Resources), DefaultQuota: Quota{Offers: 4, Executions: 2}}
	for class := range capacity {
		p.Profiles[class] = r1sruntime.Resources{MemoryBytes: 256 << 20, CPUMilli: 1000, Pids: 128}
	}
	if reader != nil {
		d := json.NewDecoder(io.LimitReader(reader, 1<<20))
		d.DisallowUnknownFields()
		if err := d.Decode(&p); err != nil {
			return p, err
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return p, fmt.Errorf("policy must contain one JSON object")
		}
	}
	for class := range capacity {
		if p.Profiles[class] == (r1sruntime.Resources{}) {
			return p, fmt.Errorf("class %q needs a bounded profile", class)
		}
	}
	return p, p.validate(capacity)
}

func (p AdmissionPolicy) validate(capacity map[string]uint32) error {
	for class, r := range p.Profiles {
		if capacity[class] == 0 {
			return fmt.Errorf("profile %q has no capacity", class)
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	validIdentity := func(id string) bool {
		b, err := hex.DecodeString(id)
		return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
	}
	for _, id := range p.AllowedClients {
		if !validIdentity(id) {
			return fmt.Errorf("allowed client must be a lowercase 16-byte identity hash")
		}
	}
	for id := range p.ClientQuotas {
		if !validIdentity(id) {
			return fmt.Errorf("quota identity must be a lowercase 16-byte identity hash")
		}
	}
	return nil
}

func (a *Allocator) admitLocked(owner []byte, assigning bool) error {
	p := a.admission
	id := hex.EncodeToString(owner)
	if p.AllowedClients != nil {
		allowed := false
		for _, candidate := range p.AllowedClients {
			allowed = allowed || candidate == id
		}
		if !allowed {
			return ErrAdmission
		}
	}
	quota := p.DefaultQuota
	if specific, ok := p.ClientQuotas[id]; ok {
		quota = specific
	}
	var offers, executions uint32
	for _, offer := range a.offers {
		if offer.status == offerOutstanding && bytes.Equal(offer.client, owner) {
			offers++
		}
	}
	for _, execution := range a.executions {
		if !terminal(execution.phase) && bytes.Equal(execution.client, owner) {
			executions++
		}
	}
	if assigning && quota.Executions > 0 && executions >= quota.Executions {
		return ErrCapacityExhausted
	}
	if !assigning && quota.Offers > 0 && offers >= quota.Offers {
		return ErrCapacityExhausted
	}
	return nil
}
