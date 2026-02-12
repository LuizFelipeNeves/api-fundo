package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// FundDetailsCollector collects detailed information about a fund
type FundDetailsCollector struct {
	client *http.Client
}

// NewFundDetailsCollector creates a new fund details collector
func NewFundDetailsCollector() *FundDetailsCollector {
	return &FundDetailsCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *FundDetailsCollector) Name() string {
	return "fund_details"
}

// FundDetails represents detailed fund information
type FundDetails struct {
	ID                   string  `json:"id"`
	Code                 string  `json:"code"`
	RazaoSocial          string  `json:"razao_social"`
	CNPJ                 string  `json:"cnpj"`
	PublicoAlvo          string  `json:"publico_alvo"`
	Mandato              string  `json:"mandato"`
	Segmento             string  `json:"segmento"`
	TipoFundo            string  `json:"tipo_fundo"`
	PrazoDuracao         string  `json:"prazo_duracao"`
	TipoGestao           string  `json:"tipo_gestao"`
	TaxaAdministracao    string  `json:"taxa_adminstracao"`
	Vacancia             float64 `json:"vacancia"`
	NumeroCotistas       int     `json:"numero_cotistas"`
	CotasEmitidas        int64   `json:"cotas_emitidas"`
	ValorPatrimonialCota float64 `json:"valor_patrimonial_cota"`
	ValorPatrimonial     float64 `json:"valor_patrimonial"`
	UltimoRendimento     float64 `json:"ultimo_rendimento"`
}

// Collect fetches fund details from the API
func (c *FundDetailsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	if req.FundCode == "" {
		return nil, fmt.Errorf("fund_code is required")
	}

	url := fmt.Sprintf("https://investidor10.com.br/api/fundos-imobiliarios/%s", req.FundCode)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch fund details: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var details FundDetails
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &CollectResult{
		FundCode:  req.FundCode,
		Data:      details,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
