package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
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
	if strings.TrimSpace(request.RunID) == "" || request.Attempt == 0 {
		return executionSpec{}, fmt.Errorf("%w: run ID and positive attempt are required", ErrInvalidRequest)
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
	// Execution lifetime is bounded by the allocator's lease sweep, never by a
	// request-time deadline, so Start runs under the caller's context only.
	return parent, func() {}
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
	writeHashPart(hash, []byte(request.RunID))
	var attempt [8]byte
	binary.BigEndian.PutUint64(attempt[:], request.Attempt)
	writeHashPart(hash, attempt[:])
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

func environment(workload *r1sv1.Workload, runID string, attempt uint64) []string {
	keys := make([]string, 0, len(workload.GetEnvironment()))
	for key := range workload.GetEnvironment() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys)+2)
	for _, key := range keys {
		if key == runEnv || key == attemptEnv {
			continue
		}
		values = append(values, key+"="+workload.GetEnvironment()[key])
	}
	values = append(values, runEnv+"="+runID, attemptEnv+"="+strconv.FormatUint(attempt, 10))
	return values
}
