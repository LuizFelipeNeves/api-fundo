package collectors

import (
	"encoding/json"
	"testing"
)

func TestFundListResponse_UnmarshalJSON_Array(t *testing.T) {
	raw := []byte(`[{"name":"ABCD11","sector":"x","p_vp":1.2,"dividend_yield":3.4,"dividend_yield_last_5_years":5.6,"daily_liquidity":7.8,"net_worth":9.1,"type":"FII"}]`)
	var r FundListResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if r.Total != 1 {
		t.Fatalf("expected total=1, got %d", r.Total)
	}
	if len(r.Data) != 1 {
		t.Fatalf("expected 1 item, got %d", len(r.Data))
	}
	if r.Data[0].Name != "ABCD11" {
		t.Fatalf("expected name=ABCD11, got %q", r.Data[0].Name)
	}
}

func TestFundListResponse_UnmarshalJSON_Object(t *testing.T) {
	raw := []byte(`{"total":2,"data":[{"name":"XPTA11","sector":"","p_vp":0,"dividend_yield":0,"dividend_yield_last_5_years":0,"daily_liquidity":0,"net_worth":0,"type":""},{"name":"YBRC11","sector":"","p_vp":0,"dividend_yield":0,"dividend_yield_last_5_years":0,"daily_liquidity":0,"net_worth":0,"type":""}]}`)
	var r FundListResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if r.Total != 2 {
		t.Fatalf("expected total=2, got %d", r.Total)
	}
	if len(r.Data) != 2 {
		t.Fatalf("expected 2 items, got %d", len(r.Data))
	}
	if r.Data[1].Name != "YBRC11" {
		t.Fatalf("expected name=YBRC11, got %q", r.Data[1].Name)
	}
}
