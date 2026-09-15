package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/proto"
)

// executionSpec is the validated, identity-bound description consumed by the
// container lifecycle layer. Its fingerprint excludes the local execution ID
// so retries can verify workload identity independently of container naming.
type executionSpec struct {
	request     r1sruntime.StartRequest
	fingerprint string
	startedAt   time.Time
}

func prepareExecution(request r1sruntime.StartRequest, reporter r1sruntime.Reporter, now time.Time, allowExpired bool) (executionSpec, error) {
	if strings.TrimSpace(request.ExecutionID) == "" {
		return executionSpec{}, fmt.Errorf("%w: execution ID is required", ErrInvalidRequest)
	}
	if reporter == nil {
		return executionSpec{}, fmt.Errorf("%w: completion reporter is required", ErrInvalidRequest)
	}
	if request.Workload == nil || request.Policy == nil {
		return executionSpec{}, fmt.Errorf("%w: workload and policy are required", ErrInvalidRequest)
	}
	if _, err := pinnedDigest(request.Workload.GetImage()); err != nil {
		return executionSpec{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if deadline := request.Policy.GetDeadline(); deadline != nil {
		if err := deadline.CheckValid(); err != nil {
			return executionSpec{}, fmt.Errorf("%w: invalid deadline: %v", ErrInvalidRequest, err)
		}
		if !deadline.AsTime().After(now) && !allowExpired {
			return executionSpec{}, ErrDeadlineExceeded
		}
	}
	if maximum := request.Policy.GetMaxRuntime(); maximum != nil {
		if err := maximum.CheckValid(); err != nil || maximum.AsDuration() <= 0 {
			return executionSpec{}, fmt.Errorf("%w: max runtime must be a positive duration", ErrInvalidRequest)
		}
	}
	if request.Policy.GetDeadline() == nil && request.Policy.GetMaxRuntime() == nil {
		return executionSpec{}, fmt.Errorf("%w: deadline or max runtime is required", ErrInvalidRequest)
	}
	if err := request.Resources.Validate(); err != nil {
		return executionSpec{}, err
	}

	fingerprint, err := fingerprint(request)
	if err != nil {
		return executionSpec{}, err
	}
	return executionSpec{request: request, fingerprint: fingerprint, startedAt: request.StartedAt}, nil
}

func (s executionSpec) startContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline := s.request.Policy.GetDeadline(); deadline != nil {
		return context.WithDeadline(parent, deadline.AsTime())
	}
	return parent, func() {}
}

func (s executionSpec) deadline() (time.Time, bool) {
	return executionDeadline(s.request.Policy, s.startedAt)
}

func (s executionSpec) started(now time.Time) executionSpec {
	if s.startedAt.IsZero() {
		s.startedAt = now
	}
	return s
}

func fingerprint(request r1sruntime.StartRequest) (string, error) {
	marshal := proto.MarshalOptions{Deterministic: true}
	workload, err := marshal.Marshal(request.Workload)
	if err != nil {
		return "", fmt.Errorf("%w: marshal workload: %v", ErrInvalidRequest, err)
	}
	policy, err := marshal.Marshal(request.Policy)
	if err != nil {
		return "", fmt.Errorf("%w: marshal policy: %v", ErrInvalidRequest, err)
	}
	hash := sha256.New()
	writeHashPart(hash, request.Client)
	writeHashPart(hash, workload)
	writeHashPart(hash, policy)
	if request.Resources != (r1sruntime.Resources{}) {
		resources, _ := json.Marshal(request.Resources)
		writeHashPart(hash, resources)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// validateAndFingerprint remains the narrow helper used by integration tests.
func validateAndFingerprint(request r1sruntime.StartRequest, reporter r1sruntime.Reporter, now time.Time, allowExpired ...bool) (string, error) {
	spec, err := prepareExecution(request, reporter, now, len(allowExpired) > 0 && allowExpired[0])
	return spec.fingerprint, err
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashPart(writer hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func executionDeadline(policy *r1sv1.ExecutionPolicy, startedAt time.Time) (time.Time, bool) {
	var deadline time.Time
	if policy.GetDeadline() != nil {
		deadline = policy.GetDeadline().AsTime()
	}
	if policy.GetMaxRuntime() != nil {
		maximum := startedAt.Add(policy.GetMaxRuntime().AsDuration())
		if deadline.IsZero() || maximum.Before(deadline) {
			deadline = maximum
		}
	}
	return deadline, !deadline.IsZero()
}

func environment(workload *r1sv1.Workload) []string {
	keys := make([]string, 0, len(workload.GetEnvironment()))
	for key := range workload.GetEnvironment() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+workload.GetEnvironment()[key])
	}
	return values
}
