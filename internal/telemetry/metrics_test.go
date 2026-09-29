package telemetry_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRecordAndExpose(t *testing.T) {
	m := telemetry.NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}

	m.ObserveRequest("default")
	m.ObserveOffer("default")
	m.ObserveRejection("CAPACITY")
	m.ObserveLeaseEviction()
	m.ObserveInboundEnvelope(128)
	m.ObserveOutboundEnvelope(256)
	m.ObserveExecutionTerminal("default", "completed", 5*time.Second)
	m.ObserveDispatchLatency("request", 2*time.Millisecond)
	m.UpdateCapacity("default", 10, 8)
	m.SetActiveExecutions("default", 2)

	server, err := telemetry.StartServer("127.0.0.1:0", m)
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	defer server.Close()

	addr := server.Address()
	if addr == "" {
		t.Fatal("expected non-empty server address")
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	output := string(body)
	expectedMetrics := []string{
		"r1s_allocator_requests_total",
		"r1s_allocator_offers_total",
		"r1s_allocator_rejections_total",
		"r1s_allocator_lease_evictions_total",
		"r1s_allocator_rns_envelopes_inbound_total",
		"r1s_allocator_rns_envelopes_outbound_total",
		"r1s_allocator_rns_control_bytes_inbound_total",
		"r1s_allocator_rns_control_bytes_outbound_total",
		"r1s_allocator_executions_total",
		"r1s_allocator_execution_duration_seconds",
		"r1s_allocator_dispatch_latency_seconds",
		"r1s_allocator_capacity_slots",
		"r1s_allocator_available_slots",
		"r1s_allocator_active_executions",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("expected output to contain %q, but was missing", metric)
		}
	}
}

func TestTelemetryServerFailureDoesNotPanic(t *testing.T) {
	var s *telemetry.Server
	if err := s.Close(); err != nil {
		t.Errorf("s.Close on nil server returned error: %v", err)
	}

	s2, err := telemetry.StartServer("", nil)
	if err != nil || s2 != nil {
		t.Errorf("StartServer with empty address should return nil, nil; got %v, %v", s2, err)
	}
}

func TestStructuredLoggerRedaction(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, true)

	logger.Info("allocator.started", "daemon initialized with r1s1:inlineInMessageSecretToken", map[string]any{
		"cluster_token":  "r1s1:supersecrettokenvalue",
		"cluster_key":    "someprivatekey",
		"resource_class": "default",
		"custom_note":    "connected via token r1s1:myjoinedtokendata to cluster",
		"env":            []string{"PATH=/usr/bin", "AUTH=r1s1:secretinenv"},
		"public_key":     "0123456789abcdef",
	})

	var entry telemetry.Event
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("json unmarshal: %v\noutput: %s", err, buf.String())
	}

	if entry.Event != "allocator.started" {
		t.Errorf("event = %q, want allocator.started", entry.Event)
	}
	if strings.Contains(entry.Message, "inlineInMessageSecretToken") {
		t.Errorf("message leaked secret: %s", entry.Message)
	}
	if !strings.Contains(entry.Message, "r1s1:[REDACTED]") {
		t.Errorf("message missing redaction marker: %s", entry.Message)
	}
	if entry.Fields["cluster_token"] != "[REDACTED]" {
		t.Errorf("cluster_token was not redacted: %v", entry.Fields["cluster_token"])
	}
	if entry.Fields["cluster_key"] != "[REDACTED]" {
		t.Errorf("cluster_key was not redacted: %v", entry.Fields["cluster_key"])
	}
	if entry.Fields["public_key"] != "0123456789abcdef" {
		t.Errorf("public_key was unexpectedly redacted: %v", entry.Fields["public_key"])
	}
	if note, ok := entry.Fields["custom_note"].(string); !ok || strings.Contains(note, "myjoinedtokendata") || !strings.Contains(note, "r1s1:[REDACTED]") {
		t.Errorf("custom_note inline token was not redacted: %v", entry.Fields["custom_note"])
	}

	envSlice, ok := entry.Fields["env"].([]any)
	if !ok || len(envSlice) != 2 {
		t.Fatalf("expected env slice of len 2, got %v", entry.Fields["env"])
	}
	if strings.Contains(envSlice[1].(string), "secretinenv") || !strings.Contains(envSlice[1].(string), "r1s1:[REDACTED]") {
		t.Errorf("env secret was not redacted: %v", envSlice[1])
	}

	// Test text logging
	buf.Reset()
	textLogger := telemetry.NewLogger(&buf, false)
	textLogger.Info("test.event", "a message", map[string]any{
		"safe_field": "prefix r1s1:secretdata suffix",
	})
	textOut := buf.String()
	if strings.Contains(textOut, "secretdata") {
		t.Errorf("text logger leaked secret: %s", textOut)
	}
	if !strings.Contains(textOut, "r1s1:[REDACTED]") {
		t.Errorf("text logger missing redacted marker: %s", textOut)
	}
}

func TestBoundedLabels(t *testing.T) {
	m := telemetry.NewMetrics()
	m.ObserveRejection("INCOMPATIBLE")
	m.ObserveRejection("CAPACITY")
	m.ObserveRejection("UNAUTHORIZED")

	count := testutil.CollectAndCount(m.Registry(), "r1s_allocator_rejections_total")
	if count != 3 {
		t.Errorf("expected 3 rejections recorded, got %d", count)
	}
}
