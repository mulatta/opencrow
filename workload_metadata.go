package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

const workloadTriggerPrefix = "workload.v1 "

type WorkloadMetadata struct {
	WorkloadID     string `json:"workload_id"`
	SourceEventID  string `json:"event_id"`
	Source         string `json:"source"`
	Action         string `json:"action"`
	ResponsePolicy string `json:"response_policy,omitempty"`
}

func parseWorkloadTriggerMetadata(content string) (string, bool, error) {
	if !strings.HasPrefix(content, workloadTriggerPrefix) {
		return "", false, nil
	}
	raw := strings.TrimSpace(strings.TrimPrefix(content, workloadTriggerPrefix))
	var meta WorkloadMetadata
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return "", false, fmt.Errorf("parse workload trigger metadata: %w", err)
	}
	if meta.WorkloadID == "" {
		return "", false, fmt.Errorf("parse workload trigger metadata: workload_id is required")
	}
	if meta.SourceEventID == "" {
		return "", false, fmt.Errorf("parse workload trigger metadata: event_id is required")
	}
	if meta.Source == "" {
		return "", false, fmt.Errorf("parse workload trigger metadata: source is required")
	}
	if meta.Action == "" {
		return "", false, fmt.Errorf("parse workload trigger metadata: action is required")
	}
	metadataJSON, err := json.Marshal(meta)
	if err != nil {
		return "", false, fmt.Errorf("encode workload trigger metadata: %w", err)
	}
	return string(metadataJSON), true, nil
}

func parseInboxWorkloadMetadata(raw string) (WorkloadMetadata, bool) {
	if strings.TrimSpace(raw) == "" {
		return WorkloadMetadata{}, false
	}
	var meta WorkloadMetadata
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return WorkloadMetadata{}, false
	}
	if meta.WorkloadID == "" {
		return WorkloadMetadata{}, false
	}
	return meta, true
}
