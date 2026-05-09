package main

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkloadLifecycleEmitterSendsDatagram(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "lifecycle.sock")
	if len(socketPath) > 100 {
		t.Skipf("Unix socket path too long for this platform: %s", socketPath)
	}
	addr := net.UnixAddr{Name: socketPath, Net: "unixgram"}
	conn, err := net.ListenUnixgram("unixgram", &addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	emitter := NewWorkloadLifecycleEmitter(WorkloadEventConfig{SocketPath: socketPath, InstanceID: "noa"})
	emitter.Emit(context.Background(), Inbox{ID: 42, Attempt: 0}, WorkloadMetadata{WorkloadID: "wl-1", SourceEventID: "event-1", Source: "rss", Action: "triage"}, "running", map[string]any{"priority": int64(1)})

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFromUnix(buf)
	if err != nil {
		t.Fatal(err)
	}
	var event WorkloadLifecycleEvent
	if err := json.Unmarshal(buf[:n], &event); err != nil {
		t.Fatal(err)
	}
	if event.EventID != "opencrow:noa:42:1:running" || event.WorkloadID != "wl-1" || event.Status != "running" {
		t.Fatalf("event = %#v", event)
	}
}

func TestWorkloadLifecycleEmitterNoopWhenUnset(t *testing.T) {
	if emitter := NewWorkloadLifecycleEmitter(WorkloadEventConfig{}); emitter != nil {
		t.Fatalf("emitter = %#v, want nil", emitter)
	}
}

func TestWorkloadLifecycleEmitterMissingSocketFailsSoft(t *testing.T) {
	emitter := NewWorkloadLifecycleEmitter(WorkloadEventConfig{SocketPath: filepath.Join(t.TempDir(), "missing.sock"), InstanceID: "noa"})
	start := time.Now()
	emitter.Emit(context.Background(), Inbox{ID: 42}, WorkloadMetadata{WorkloadID: "wl-1"}, "running", nil)
	if time.Since(start) > time.Second {
		t.Fatal("missing socket emit took too long")
	}
}
