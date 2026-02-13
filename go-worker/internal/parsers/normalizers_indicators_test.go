package parsers

import "testing"

func TestNormalizeIndicatorsAny_Map(t *testing.T) {
	raw := map[string]interface{}{
		"X": []interface{}{
			map[string]interface{}{"year": "2026", "value": 1.0},
		},
	}

	got := NormalizeIndicatorsAny(raw)
	if got == nil {
		t.Fatalf("expected non-nil map")
	}
	if _, ok := got["X"]; !ok {
		t.Fatalf("expected key X")
	}
}

func TestNormalizeIndicatorsAny_Array(t *testing.T) {
	raw := []interface{}{}
	got := NormalizeIndicatorsAny(raw)
	if got == nil {
		t.Fatalf("expected non-nil map")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}
