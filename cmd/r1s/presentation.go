package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
)

func printState(writer io.Writer, executionID string, state *r1sv1.ExecutionState) {
	if state == nil {
		fmt.Fprintf(writer, "execution=%s phase=unknown\n", executionID)
		return
	}
	exit := ""
	if state.ExitCode != nil {
		exit = fmt.Sprintf(" exit_code=%d", state.GetExitCode())
	}
	fmt.Fprintf(writer, "execution=%s phase=%s occurred_at=%s%s detail=%q\n", executionID, phaseName(state.GetPhase()), state.GetOccurredAt().AsTime().Format(time.RFC3339), exit, state.GetDetail())
}

func phaseName(phase r1sv1.ExecutionPhase) string {
	return strings.ToLower(strings.TrimPrefix(phase.String(), "EXECUTION_PHASE_"))
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return protocol.Terminal(phase)
}

type stringValues []string

func (v *stringValues) String() string { return strings.Join(*v, ",") }
func (v *stringValues) Set(value string) error {
	*v = append(*v, value)
	return nil
}

const maxRequestJSONBytes = 1 << 20

func decodeRequestJSON(value string) (*r1sv1.ExecutionRequest, error) {
	if len(value) > maxRequestJSONBytes {
		return nil, fmt.Errorf("request: JSON exceeds %d bytes", maxRequestJSONBytes)
	}
	request := new(r1sv1.ExecutionRequest)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(value), request); err != nil {
		return nil, fmt.Errorf("request: decode JSON: %w", err)
	}
	if request.GetRequestId() != "" {
		return nil, errors.New("request: requestId must be omitted; the client generates it")
	}
	if strings.TrimSpace(request.GetResourceClass()) == "" {
		request.ResourceClass = "default"
	}
	return request, nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintf(output, "Usage of %s:\n", name)
		flags.VisitAll(func(candidate *flag.Flag) {
			fmt.Fprintf(output, "  --%s value\n    \t%s", candidate.Name, candidate.Usage)
			if candidate.DefValue != "" && candidate.DefValue != "false" && candidate.DefValue != "0" {
				fmt.Fprintf(output, " (default %s)", strconv.Quote(candidate.DefValue))
			}
			fmt.Fprintln(output)
		})
	}
	return flags
}
