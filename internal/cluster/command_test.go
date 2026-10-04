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

func TestTokenCommandTransfersRecordedBootstrapDestinations(t *testing.T) {
	directory := t.TempDir()
	key := bytes.Repeat([]byte{0x72}, KeySize)
	id, err := SaveCredential(directory, key)
	if err != nil {
		t.Fatal(err)
	}
	destination := strings.Repeat("4d", DestinationSize)
	if err := RecordBootstrapDestination(filepath.Join(directory, id), destination); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := RunCommand([]string{"token", id[:12]}, directory, &output, &output); err != nil {
		t.Fatal(err)
	}
	value := strings.TrimPrefix(strings.TrimSpace(output.String()), "Join token: ")
	if !strings.HasPrefix(value, "r1s1:") {
		t.Fatalf("token output = %q", output.String())
	}
	joinedDirectory := t.TempDir()
	if err := RunCommand([]string{"join", value}, joinedDirectory, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	joined, _, err := ResolveCredential(joinedDirectory, id)
	if err != nil || len(joined.BootstrapDestinations) != 1 || joined.BootstrapDestinations[0] != destination {
		t.Fatalf("joined credential = %+v, %v", joined, err)
	}
}
