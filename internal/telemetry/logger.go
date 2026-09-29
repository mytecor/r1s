package telemetry

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Level represents log severity level.
type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

var tokenPattern = regexp.MustCompile(`r1s1:[a-zA-Z0-9+/=_-]+`)

// Event is a structured log entry emitted by r1s daemon or client.
// Secret fields (keys, tokens, identities) are strictly redacted before emission.
type Event struct {
	Timestamp string         `json:"time"`
	Level     Level          `json:"level"`
	Event     string         `json:"event"`
	Message   string         `json:"message,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Logger formats and outputs structured events in JSON or logfmt-style lines.
type Logger struct {
	mu     sync.Mutex
	output io.Writer
	json   bool
}

// NewLogger constructs a structured logger writing to output.
func NewLogger(output io.Writer, asJSON bool) *Logger {
	return &Logger{
		output: output,
		json:   asJSON,
	}
}

// IsJSON reports whether this logger outputs in JSON format.
func (l *Logger) IsJSON() bool {
	if l == nil {
		return false
	}
	return l.json
}

// Log emits a structured log event with redacted fields.
func (l *Logger) Log(level Level, event, message string, fields map[string]any) {
	if l == nil || l.output == nil {
		return
	}
	message = RedactSecret(message)
	redacted := redactMap(fields)

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	if l.json {
		entry := Event{
			Timestamp: now,
			Level:     level,
			Event:     event,
			Message:   message,
			Fields:    redacted,
		}
		data, err := json.Marshal(entry)
		if err == nil {
			_, _ = fmt.Fprintln(l.output, string(data))
		}
		return
	}

	// Key-value text format: time=... level=... event=... message=... k=v ...
	var b strings.Builder
	b.WriteString("time=")
	b.WriteString(now)
	b.WriteString(" level=")
	b.WriteString(string(level))
	b.WriteString(" event=")
	b.WriteString(event)
	if message != "" {
		b.WriteString(" msg=")
		b.WriteString(fmt.Sprintf("%q", message))
	}
	for k, v := range redacted {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprintf("%v", v))
	}
	_, _ = fmt.Fprintln(l.output, b.String())
}

// Info logs an INFO level event.
func (l *Logger) Info(event, message string, fields map[string]any) {
	l.Log(LevelInfo, event, message, fields)
}

// Warn logs a WARN level event.
func (l *Logger) Warn(event, message string, fields map[string]any) {
	l.Log(LevelWarn, event, message, fields)
}

// Error logs an ERROR level event.
func (l *Logger) Error(event, message string, fields map[string]any) {
	l.Log(LevelError, event, message, fields)
}

// RedactSecret sanitizes strings containing potential tokens or sensitive key material.
func RedactSecret(value string) string {
	return tokenPattern.ReplaceAllString(value, "r1s1:[REDACTED]")
}

func isSecretKey(k string) bool {
	lower := strings.ToLower(k)
	if strings.Contains(lower, "message_id") || strings.Contains(lower, "correlation") || strings.Contains(lower, "public_key") {
		return false
	}
	return strings.Contains(lower, "secret") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "password") ||
		strings.Contains(lower, "key") ||
		strings.Contains(lower, "seed") ||
		strings.Contains(lower, "credential") ||
		strings.Contains(lower, "private")
}

func redactValue(v any) any {
	switch val := v.(type) {
	case string:
		return RedactSecret(val)
	case []string:
		out := make([]string, len(val))
		for i, s := range val {
			out[i] = RedactSecret(s)
		}
		return out
	case map[string]any:
		return redactMap(val)
	case map[string]string:
		out := make(map[string]any, len(val))
		for k, s := range val {
			if isSecretKey(k) {
				out[k] = "[REDACTED]"
			} else {
				out[k] = RedactSecret(s)
			}
		}
		return out
	default:
		return v
	}
}

func redactMap(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	res := make(map[string]any, len(fields))
	for k, v := range fields {
		if isSecretKey(k) {
			res[k] = "[REDACTED]"
			continue
		}
		res[k] = redactValue(v)
	}
	return res
}
