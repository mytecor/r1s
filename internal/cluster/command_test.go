package cluster

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitJoinAndListCommands(t *testing.T) {
	var initialized bytes.Buffer
	firstDirectory := filepath.Join(t.TempDir(), "first", "clusters")
	if err := RunCommand([]string{"init"}, firstDirectory, &initialized, &initialized); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(initialized.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "Cluster ID: ") || !strings.HasPrefix(lines[1], "Join token: r1s1:") {
		t.Fatalf("init output = %q", initialized.String())
	}
	token := strings.TrimPrefix(lines[1], "Join token: ")
	secondDirectory := filepath.Join(t.TempDir(), "second", "clusters")
	var joined bytes.Buffer
	if err := RunCommand([]string{"join", token}, secondDirectory, &joined, &joined); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(joined.String(), token) || !strings.Contains(joined.String(), lines[0]) {
		t.Fatalf("join output = %q", joined.String())
	}
	if err := RunCommand([]string{"join", token}, secondDirectory, &joined, &joined); err != nil {
		t.Fatalf("duplicate join: %v", err)
	}
	var listed bytes.Buffer
	if err := RunCommand([]string{"list"}, secondDirectory, &listed, &listed); err != nil {
		t.Fatal(err)
	}
	id := strings.TrimPrefix(lines[0], "Cluster ID: ")
	if strings.TrimSpace(listed.String()) != id || strings.Contains(listed.String(), token) {
		t.Fatalf("list output = %q", listed.String())
	}
}
