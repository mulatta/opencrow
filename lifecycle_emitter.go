package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"
)

const lifecycleEventType = "workload.lifecycle"

type WorkloadLifecycleEmitter struct {
	socketPath string
	instanceID string
	timeout    time.Duration
}

type WorkloadLifecycleEvent struct {
	Version       int            `json:"version"`
	Type          string         `json:"type"`
	EventID       string         `json:"event_id"`
	WorkloadID    string         `json:"workload_id"`
	SourceEventID string         `json:"source_event_id,omitempty"`
	Source        string         `json:"source,omitempty"`
	Action        string         `json:"action,omitempty"`
	Status        string         `json:"status"`
	OccurredAt    string         `json:"occurred_at"`
	Instance      string         `json:"opencrow_instance,omitempty"`
	InboxID       int64          `json:"inbox_id,omitempty"`
	Attempt       int64          `json:"attempt,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

func NewWorkloadLifecycleEmitter(cfg WorkloadEventConfig) *WorkloadLifecycleEmitter {
	if cfg.SocketPath == "" {
		return nil
	}
	return &WorkloadLifecycleEmitter{
		socketPath: cfg.SocketPath,
		instanceID: cfg.InstanceID,
		timeout:    100 * time.Millisecond,
	}
}

func (e *WorkloadLifecycleEmitter) Emit(_ context.Context, item Inbox, metadata WorkloadMetadata, status string, details map[string]any) {
	if e == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	attempt := item.Attempt + 1
	event := WorkloadLifecycleEvent{
		Version:       1,
		Type:          lifecycleEventType,
		EventID:       fmt.Sprintf("opencrow:%s:%d:%d:%s", e.instanceID, item.ID, attempt, status),
		WorkloadID:    metadata.WorkloadID,
		SourceEventID: metadata.SourceEventID,
		Source:        metadata.Source,
		Action:        metadata.Action,
		Status:        status,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Instance:      e.instanceID,
		InboxID:       item.ID,
		Attempt:       attempt,
		Details:       details,
	}
	// Lifecycle audit should survive prompt cancellation/preemption. The
	// emitter has its own short deadline, so do not inherit a cancelled item ctx.
	if err := e.emit(context.Background(), event); err != nil {
		slog.Warn("workload lifecycle emit failed", "workload_id", metadata.WorkloadID, "status", status, "error", err)
	}
}

func (e *WorkloadLifecycleEmitter) emit(ctx context.Context, event WorkloadLifecycleEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode lifecycle event: %w", err)
	}
	dialer := net.Dialer{Timeout: e.timeout}
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "unixgram", e.socketPath)
	if err != nil {
		return fmt.Errorf("dial lifecycle socket: %w", err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(e.timeout)); err != nil {
		return fmt.Errorf("set lifecycle write deadline: %w", err)
	}
	if _, err := conn.Write(raw); err != nil {
		return fmt.Errorf("write lifecycle event: %w", err)
	}
	return nil
}
