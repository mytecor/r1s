//go:build linux

package containerd

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

func TestTunnelRefusesAllocatorNamespace(t *testing.T) {
	namespace, err := os.Open("/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	conn, err := dialNamespace(context.Background(), namespace, 80)
	if conn != nil || err == nil || !strings.Contains(err.Error(), "allocator network namespace") {
		t.Fatalf("host namespace dial = %v, %v", conn, err)
	}
}

func TestContainerdTunnelExecutionIsolation(t *testing.T) {
	if os.Getenv("RUN_CONTAINERD_INTEGRATION") != "1" {
		t.Skip("set RUN_CONTAINERD_INTEGRATION=1 for live containerd")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	adapter, err := New(ctx, Config{Address: os.Getenv("CONTAINERD_ADDRESS"), Namespace: "r1s-tunnel-integration", Snapshotter: os.Getenv("CONTAINERD_SNAPSHOTTER")})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "allocator-host") }))
	defer host.Close()
	_, portText, _ := net.SplitHostPort(host.Listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	// The host and both containers use the same TCP port. Only the selected
	// execution's response may be returned.
	for _, marker := range []string{"execution-one", "execution-two"} {
		request := testRequest(fmt.Sprintf("tunnel-%s-%d", marker, time.Now().UnixNano()))
		request.Workload.Image = image
		request.Workload.Args = []string{fmt.Sprintf("while true; do printf '%s' | nc -l -p %d >/dev/null; done", marker, port)}
		if err := adapter.Start(ctx, request, func(r1sruntime.Completion) error { return nil }); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := adapter.Stop(cleanup, request.ExecutionID); err != nil {
				t.Error(err)
			}
		}()
		var conn net.Conn
		for {
			conn, err = adapter.DialExecution(ctx, request.ExecutionID, uint16(port))
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("container port: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		fmt.Fprint(conn, "GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
		response := make([]byte, len(marker))
		_, readErr := io.ReadFull(conn, response)
		conn.Close()
		if readErr != nil || !strings.Contains(string(response), marker) || strings.Contains(string(response), "allocator-host") {
			t.Fatalf("execution response: %q, %v", response, readErr)
		}
	}
	if _, err := adapter.DialExecution(ctx, "missing-execution", uint16(port)); err == nil {
		t.Fatal("unknown execution connected")
	}
}
