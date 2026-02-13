package scheduler

import "testing"

func TestLatestTodayPrice_Array(t *testing.T) {
	data := []byte(`[
		{"price": 10.1, "hour": "10:15"},
		{"price": 10.7, "hour": "18:05"},
		{"price": 10.6, "hour": "17:59"}
	]`)
	price, ok := latestTodayPrice(data)
	if !ok {
		t.Fatalf("expected ok")
	}
	if price != 10.7 {
		t.Fatalf("expected 10.7, got %v", price)
	}
}

func TestLatestTodayPrice_WrapperReal(t *testing.T) {
	data := []byte(`{"real":[
		{"price": 9.9, "hour": "09:00"},
		{"price": 11.2, "hour": "18:30"}
	]}`)
	price, ok := latestTodayPrice(data)
	if !ok {
		t.Fatalf("expected ok")
	}
	if price != 11.2 {
		t.Fatalf("expected 11.2, got %v", price)
	}
}

func TestLatestTodayPrice_HourUnixMillis(t *testing.T) {
	data := []byte(`[
		{"price": 10.0, "hour": 1700000000000},
		{"price": 12.0, "hour": 1700003600000}
	]`)
	price, ok := latestTodayPrice(data)
	if !ok {
		t.Fatalf("expected ok")
	}
	if price != 12.0 {
		t.Fatalf("expected 12.0, got %v", price)
	}
}
