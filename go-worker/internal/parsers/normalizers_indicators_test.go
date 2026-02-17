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
	series, ok := got["X"]
	if !ok {
		t.Fatalf("expected key X")
	}
	if len(series) != 1 {
		t.Fatalf("expected 1 item, got %d", len(series))
	}
	if series[0].Year != "2026" {
		t.Fatalf("expected year=2026, got %q", series[0].Year)
	}
	if series[0].Value == nil || *series[0].Value != 1.0 {
		t.Fatalf("expected value=1.0, got %+v", series[0].Value)
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

func TestNormalizeIndicatorsAny_MapsKnownKeys(t *testing.T) {
	raw := map[string]interface{}{
		"COTAS EMITIDAS": []interface{}{
			map[string]interface{}{"year": "Atual", "value": nil},
		},
	}

	got := NormalizeIndicatorsAny(raw)
	if _, ok := got["cotas_emitidas"]; !ok {
		t.Fatalf("expected key cotas_emitidas")
	}
	if _, ok := got["COTAS EMITIDAS"]; ok {
		t.Fatalf("did not expect original key COTAS EMITIDAS")
	}
	series := got["cotas_emitidas"]
	if len(series) != 1 {
		t.Fatalf("expected 1 item, got %d", len(series))
	}
	if series[0].Year != "Atual" {
		t.Fatalf("expected year=Atual, got %q", series[0].Year)
	}
	if series[0].Value != nil {
		t.Fatalf("expected nil value, got %+v", series[0].Value)
	}
}
