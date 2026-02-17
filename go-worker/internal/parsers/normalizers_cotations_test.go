package parsers

import (
	"encoding/json"
	"testing"
)

func TestNormalizeCotations_AcceptsCreatedAtField(t *testing.T) {
	var raw map[string][]interface{}
	if err := json.Unmarshal([]byte(`{
		"real":[
			{"price":73.73,"created_at":"09/12/2021"},
			{"price":"73,74","created_at":"10/12/2021"}
		]
	}`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotations(raw)
	if got == nil {
		t.Fatalf("expected non-nil result")
	}
	if len(got.Real) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got.Real))
	}
	if got.Real[0].Date != "09/12/2021" || got.Real[0].Price != 73.73 {
		t.Fatalf("unexpected first item: %+v", got.Real[0])
	}
	if got.Real[1].Date != "10/12/2021" || got.Real[1].Price != 73.74 {
		t.Fatalf("unexpected second item: %+v", got.Real[1])
	}
}

func TestToDateISO_AcceptsDateTimePrefix(t *testing.T) {
	if got := ToDateISO("09/12/2021 10:05:00"); got != "2021-12-09" {
		t.Fatalf("expected 2021-12-09, got %q", got)
	}
}

func TestToDateISO_PassesThroughISO(t *testing.T) {
	if got := ToDateISO("2021-12-09"); got != "2021-12-09" {
		t.Fatalf("expected 2021-12-09, got %q", got)
	}
}
