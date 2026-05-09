package main

import "testing"

func TestParseWorkloadTriggerMetadata(t *testing.T) {
	metadataJSON, ok, err := parseWorkloadTriggerMetadata(`workload.v1 {"workload_id":"wl-1","event_id":"event-1","source":"rss","action":"triage","response_policy":"notify"}`)
	if err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	if !ok {
		t.Fatal("metadata not detected")
	}
	meta, ok := parseInboxWorkloadMetadata(metadataJSON)
	if !ok {
		t.Fatalf("stored metadata did not parse: %s", metadataJSON)
	}
	if meta.WorkloadID != "wl-1" || meta.SourceEventID != "event-1" || meta.Source != "rss" || meta.Action != "triage" || meta.ResponsePolicy != "notify" {
		t.Fatalf("metadata = %#v", meta)
	}
}

func TestParseWorkloadTriggerMetadataIgnoresPlainTrigger(t *testing.T) {
	_, ok, err := parseWorkloadTriggerMetadata("plain trigger")
	if err != nil {
		t.Fatalf("plain trigger returned error: %v", err)
	}
	if ok {
		t.Fatal("plain trigger detected as workload")
	}
}

func TestParseWorkloadTriggerMetadataRejectsMalformedWorkload(t *testing.T) {
	_, ok, err := parseWorkloadTriggerMetadata("workload.v1 not-json")
	if err == nil {
		t.Fatal("malformed workload trigger returned nil error")
	}
	if ok {
		t.Fatal("malformed workload trigger detected as metadata")
	}
}
