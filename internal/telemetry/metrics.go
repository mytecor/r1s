package telemetry

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics manages the Prometheus registry and metric collectors for an r1s allocator.
// Label values are strictly bounded to prevent cardinality explosion:
//   - resource_class: operator-configured resource classes (e.g. "default", "gpu")
//     plus the allocator's fixed "__unconfigured__" fallback
//   - phase: protocol execution phases (e.g. "running", "completed", "failed")
//   - reason: categorical rejection/error codes (e.g. "CAPACITY", "INCOMPATIBLE", "ADMISSION", "UNAUTHORIZED", "NOT_FOUND", "EXPIRED", "CONFLICT")
//
// Dynamic runtime IDs, message IDs, hashes, join tokens, and container output are NEVER used as labels.
type Metrics struct {
	reg *prometheus.Registry

	requestsTotal        *prometheus.CounterVec
	offersTotal          *prometheus.CounterVec
	executionsTotal      *prometheus.CounterVec
	rejectionsTotal      *prometheus.CounterVec
	leaseEvictionsTotal  prometheus.Counter
	rnsEnvelopesInbound  prometheus.Counter
	rnsEnvelopesOutbound prometheus.Counter
	rnsBytesInbound      prometheus.Counter
	rnsBytesOutbound     prometheus.Counter
	executionDuration    *prometheus.HistogramVec
	dispatchLatency      *prometheus.HistogramVec
	activeExecutions     *prometheus.GaugeVec
	capacitySlots        *prometheus.GaugeVec
	availableSlots       *prometheus.GaugeVec
}

// NewMetrics constructs and registers allocator metrics on a dedicated Prometheus registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		reg: reg,

		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "requests_total",
			Help:      "Total execution requests evaluated by the allocator by resource class.",
		}, []string{"resource_class"}),

		offersTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "offers_total",
			Help:      "Total execution offers minted by the allocator by resource class.",
		}, []string{"resource_class"}),

		executionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "executions_total",
			Help:      "Total execution attempts reaching terminal phases by resource class and phase.",
		}, []string{"resource_class", "phase"}),

		rejectionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "rejections_total",
			Help:      "Total command rejections by categorical reason.",
		}, []string{"reason"}),

		leaseEvictionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "lease_evictions_total",
			Help:      "Total executions evicted due to unrenewed client leases.",
		}),

		rnsEnvelopesInbound: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "rns_envelopes_inbound_total",
			Help:      "Total inbound RNS control envelopes received.",
		}),

		rnsEnvelopesOutbound: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "rns_envelopes_outbound_total",
			Help:      "Total outbound RNS control envelopes sent.",
		}),

		rnsBytesInbound: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "rns_control_bytes_inbound_total",
			Help:      "Total inbound RNS control bytes received.",
		}),

		rnsBytesOutbound: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "rns_control_bytes_outbound_total",
			Help:      "Total outbound RNS control bytes sent.",
		}),

		executionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "execution_duration_seconds",
			Help:      "Execution duration in seconds from assignment to terminal state.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600},
		}, []string{"resource_class", "phase"}),

		dispatchLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "dispatch_latency_seconds",
			Help:      "Latency of command handling in seconds by command type.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"command"}),

		activeExecutions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "active_executions",
			Help:      "Current number of active (non-terminal) executions by resource class.",
		}, []string{"resource_class"}),

		capacitySlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "capacity_slots",
			Help:      "Configured total slot capacity by resource class.",
		}, []string{"resource_class"}),

		availableSlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "r1s",
			Subsystem: "allocator",
			Name:      "available_slots",
			Help:      "Currently unreserved slots by resource class.",
		}, []string{"resource_class"}),
	}

	reg.MustRegister(
		m.requestsTotal,
		m.offersTotal,
		m.executionsTotal,
		m.rejectionsTotal,
		m.leaseEvictionsTotal,
		m.rnsEnvelopesInbound,
		m.rnsEnvelopesOutbound,
		m.rnsBytesInbound,
		m.rnsBytesOutbound,
		m.executionDuration,
		m.dispatchLatency,
		m.activeExecutions,
		m.capacitySlots,
		m.availableSlots,
	)

	return m
}

// Registry returns the underlying prometheus Registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.reg
}

// Handler returns an http.Handler that serves the registered metrics in Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveRequest records an incoming execution request for a resource class.
func (m *Metrics) ObserveRequest(resourceClass string) {
	if m == nil {
		return
	}
	if resourceClass == "" {
		resourceClass = "default"
	}
	m.requestsTotal.WithLabelValues(resourceClass).Inc()
}

// ObserveOffer records an issued offer for a resource class.
func (m *Metrics) ObserveOffer(resourceClass string) {
	if m == nil {
		return
	}
	if resourceClass == "" {
		resourceClass = "default"
	}
	m.offersTotal.WithLabelValues(resourceClass).Inc()
}

// ObserveRejection records a command rejection categorized by reason code.
func (m *Metrics) ObserveRejection(reason string) {
	if m == nil {
		return
	}
	if reason == "" {
		reason = "UNKNOWN"
	}
	m.rejectionsTotal.WithLabelValues(reason).Inc()
}

// ObserveLeaseEviction records an execution evicted due to lease expiry.
func (m *Metrics) ObserveLeaseEviction() {
	if m == nil {
		return
	}
	m.leaseEvictionsTotal.Inc()
}

// ObserveInboundEnvelope records an inbound RNS envelope and payload size.
func (m *Metrics) ObserveInboundEnvelope(bytes int) {
	if m == nil {
		return
	}
	m.rnsEnvelopesInbound.Inc()
	if bytes > 0 {
		m.rnsBytesInbound.Add(float64(bytes))
	}
}

// ObserveOutboundEnvelope records an outbound RNS envelope and payload size.
func (m *Metrics) ObserveOutboundEnvelope(bytes int) {
	if m == nil {
		return
	}
	m.rnsEnvelopesOutbound.Inc()
	if bytes > 0 {
		m.rnsBytesOutbound.Add(float64(bytes))
	}
}

// ObserveExecutionTerminal records an execution transitioning to a terminal phase.
func (m *Metrics) ObserveExecutionTerminal(resourceClass, phase string, duration time.Duration) {
	if m == nil {
		return
	}
	if resourceClass == "" {
		resourceClass = "default"
	}
	m.executionsTotal.WithLabelValues(resourceClass, phase).Inc()
	if duration >= 0 {
		m.executionDuration.WithLabelValues(resourceClass, phase).Observe(duration.Seconds())
	}
}

// ObserveDispatchLatency records the duration of command dispatch handling.
func (m *Metrics) ObserveDispatchLatency(command string, duration time.Duration) {
	if m == nil {
		return
	}
	if command == "" {
		command = "unknown"
	}
	if duration >= 0 {
		m.dispatchLatency.WithLabelValues(command).Observe(duration.Seconds())
	}
}

// UpdateCapacity gauges sets the configured total and available slots per class.
func (m *Metrics) UpdateCapacity(class string, total, available uint32) {
	if m == nil {
		return
	}
	if class == "" {
		class = "default"
	}
	m.capacitySlots.WithLabelValues(class).Set(float64(total))
	m.availableSlots.WithLabelValues(class).Set(float64(available))
}

// SetActiveExecutions updates the current number of non-terminal executions for a class.
func (m *Metrics) SetActiveExecutions(class string, count int) {
	if m == nil {
		return
	}
	if class == "" {
		class = "default"
	}
	m.activeExecutions.WithLabelValues(class).Set(float64(count))
}

// Server serves Prometheus metrics over HTTP.
// Failure or disconnection of this server never impacts allocator operation or workload execution.
type Server struct {
	server   *http.Server
	listener net.Listener
}

// StartServer starts an HTTP metrics exporter on address listening at /metrics.
// An empty address disables the metrics server and returns nil.
func StartServer(address string, metrics *Metrics) (*Server, error) {
	if address == "" || metrics == nil {
		return nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s := &Server{
		server:   httpServer,
		listener: listener,
	}
	go func() {
		_ = httpServer.Serve(listener)
	}()
	return s, nil
}

// Address returns the listener's network address, useful when listening on :0.
func (s *Server) Address() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Close stops the HTTP metrics server gracefully.
func (s *Server) Close() error {
	if s == nil || s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		_ = s.server.Close()
	}
	return err
}
