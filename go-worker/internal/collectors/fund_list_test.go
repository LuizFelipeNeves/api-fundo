package collectors

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
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

func TestFundListResponse_UnmarshalJSON_DataTables(t *testing.T) {
	raw := []byte(`{"draw":1,"recordsTotal":2,"recordsFiltered":2,"data":[{"name":"XPTA11","sector":"","p_vp":0,"dividend_yield":0,"dividend_yield_last_5_years":0,"daily_liquidity":0,"net_worth":0,"type":""},{"name":"YBRC11","sector":"","p_vp":0,"dividend_yield":0,"dividend_yield_last_5_years":0,"daily_liquidity":0,"net_worth":0,"type":""}]}`)
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

func TestFundListResponse_UnmarshalJSON_AdvancedSearch(t *testing.T) {
	raw := []byte(`{"draw":3,"total":492,"data":[{"name":"XPIE11","sector":"Fundo de Investimentos em Participações (FIP)","p_vp":0.74,"dividend_yield":12.55,"dividend_yield_last_5_years":8.26,"daily_liquidity":1240412,"net_worth":1069221955.92,"type":"Outro","id":3673168}]}`)
	var r FundListResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if r.Total != 492 {
		t.Fatalf("expected total=492, got %d", r.Total)
	}
	if len(r.Data) != 1 {
		t.Fatalf("expected 1 item, got %d", len(r.Data))
	}
	if r.Data[0].Name != "XPIE11" {
		t.Fatalf("expected name=XPIE11, got %q", r.Data[0].Name)
	}
}

func TestFundListCollector_LiveInvestidor10(t *testing.T) {
	if os.Getenv("LIVE_INVESTIDOR10") != "1" {
		t.Skip("set LIVE_INVESTIDOR10=1 to run")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	cfg.HTTPTimeoutMS = 25000
	cfg.HTTPRetryMax = 3
	cfg.HTTPRetryDelayMS = 1500

	client, err := httpclient.New(cfg)
	if err != nil {
		t.Fatalf("failed to create http client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	collector := NewFundListCollector(client)
	res, err := collector.Collect(ctx, CollectRequest{})
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	items, ok := res.Data.([]FundListItem)
	if !ok {
		t.Fatalf("expected []FundListItem, got %T", res.Data)
	}
	if len(items) == 0 {
		t.Fatalf("expected non-empty fund list")
	}
	if items[0].Code == "" {
		t.Fatalf("expected first fund code to be non-empty")
	}
}
