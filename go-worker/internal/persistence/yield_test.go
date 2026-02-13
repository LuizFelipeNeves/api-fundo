package persistence

import "testing"

func TestCalcYield(t *testing.T) {
	if got := calcYield(0, 10); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
	if got := calcYield(1, 0); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}

	got := calcYield(1, 20)
	want := 5.0
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
