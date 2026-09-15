package cluster

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitJoinAndShowCommands(t *testing.T) {
	var initialized bytes.Buffer
	firstPath := filepath.Join(t.TempDir(), "first", "cluster")
	if err := RunCommand([]string{"init"}, firstPath, &initialized, &initialized); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(initialized.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "Cluster ID: ") || !strings.HasPrefix(lines[1], "Join token: r1s1:") {
		t.Fatalf("init output = %q", initialized.String())
	}
	token := strings.TrimPrefix(lines[1], "Join token: ")
	secondPath := filepath.Join(t.TempDir(), "second", "cluster")
	var joined bytes.Buffer
	if err := RunCommand([]string{"join", token}, secondPath, &joined, &joined); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(joined.String(), token) || !strings.Contains(joined.String(), lines[0]) {
		t.Fatalf("join output = %q", joined.String())
	}
	var shown bytes.Buffer
	if err := RunCommand([]string{"show"}, secondPath, &shown, &shown); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(shown.String(), lines[0]) || strings.Contains(shown.String(), token) {
		t.Fatalf("show output = %q", shown.String())
	}
}
