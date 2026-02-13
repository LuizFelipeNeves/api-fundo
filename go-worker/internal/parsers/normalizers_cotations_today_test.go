package parsers

import (
	"encoding/json"
	"testing"
)

func TestNormalizeCotationsToday_Object(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`{"price": 10.5}`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if _, ok := got.(map[string]interface{}); !ok {
		t.Fatalf("expected map[string]interface{}, got %T", got)
	}
}

func TestNormalizeCotationsToday_Array(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`[{"price": 10.5}]`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if _, ok := got.([]interface{}); !ok {
		t.Fatalf("expected []interface{}, got %T", got)
	}
}
