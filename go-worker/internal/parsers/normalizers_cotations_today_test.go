package parsers

import (
	"encoding/json"
	"testing"
)

func TestNormalizeCotationsToday_NormalizaFormatoInvestidor10(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`{"real":[{"price":10.5,"created_at":"2026-02-02 10:01:00"}]}`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}
	if got[0].Price != 10.5 || got[0].Hour != "10:01" {
		t.Fatalf("unexpected item: %+v", got[0])
	}
}

func TestNormalizeCotationsToday_RemoveDuplicadosEOrdenaFormatoInvestidor10(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`{"real":[{"price":10,"created_at":"2026-02-02 10:02:00"},{"price":9,"created_at":"2026-02-02 10:01:00"},{"price":11,"created_at":"2026-02-02 10:02:59"}]}`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
	if got[0].Hour != "10:01" || got[0].Price != 9 {
		t.Fatalf("unexpected first item: %+v", got[0])
	}
	if got[1].Hour != "10:02" || got[1].Price != 11 {
		t.Fatalf("unexpected second item: %+v", got[1])
	}
}

func TestNormalizeCotationsToday_NormalizaFormatoStatusInvest(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`[
		{"currencyType":1,"currency":"Real brasileiro","symbol":"R$","prices":[{"price":6.62,"date":"02/02/2026 10:05:00"},{"value":"6,63","hour":"10:06"}]}
	]`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
	if got[0].Hour != "10:05" || got[0].Price != 6.62 {
		t.Fatalf("unexpected first item: %+v", got[0])
	}
	if got[1].Hour != "10:06" || got[1].Price != 6.63 {
		t.Fatalf("unexpected second item: %+v", got[1])
	}
}

func TestNormalizeCotationsToday_RemoveDuplicadosEOrdenaFormatoStatusInvest(t *testing.T) {
	var raw interface{}
	if err := json.Unmarshal([]byte(`[
		{"currencyType":1,"prices":[{"value":"6,61","hour":"10:05"},{"price":6.6,"date":"02/02/2026 10:04:00"},{"price":6.62,"hour":"10:05"}]}
	]`), &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	got := NormalizeCotationsToday(raw)
	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
	if got[0].Hour != "10:04" || got[0].Price != 6.6 {
		t.Fatalf("unexpected first item: %+v", got[0])
	}
	if got[1].Hour != "10:05" || got[1].Price != 6.62 {
		t.Fatalf("unexpected second item: %+v", got[1])
	}
}
