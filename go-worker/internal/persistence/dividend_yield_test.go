package persistence

import (
	"sort"
	"testing"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/analytics"
)

func TestSimulateRealScenario(t *testing.T) {
	// Simulando o cenário real do LIFE11

	// Cotações dos últimos 5 anos (1825 dias)
	// Vamos simular os preços de fechamento de cada mês
	allPrices := []struct {
		date  time.Time
		price int
	}{
		// Fev/2025 - precisamos ver se existe preço
		{time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC), 0}, // Preço zero = não existe!
		{time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC), 89000},
		{time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), 88000},
		{time.Date(2024, 11, 29, 0, 0, 0, 0, time.UTC), 87000},
		{time.Date(2024, 10, 31, 0, 0, 0, 0, time.UTC), 86000},
		{time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC), 85000},
		{time.Date(2024, 8, 30, 0, 0, 0, 0, time.UTC), 84000},
		{time.Date(2024, 7, 31, 0, 0, 0, 0, time.UTC), 83000},
		{time.Date(2024, 6, 28, 0, 0, 0, 0, time.UTC), 82000},
		{time.Date(2024, 5, 31, 0, 0, 0, 0, time.UTC), 81000},
		{time.Date(2024, 4, 30, 0, 0, 0, 0, time.UTC), 80000},
		{time.Date(2024, 3, 28, 0, 0, 0, 0, time.UTC), 79000},
		{time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), 78000},
	}

	// Como no código real
	dates := []time.Time{}
	prices := []float64{}
	monthLastPrice := map[string]float64{}

	for i := len(allPrices) - 1; i >= 0; i-- {
		pf := float64(allPrices[i].price) / 10000.0
		if pf <= 0 {
			continue
		}
		dates = append(dates, allPrices[i].date)
		prices = append(prices, pf)
		monthLastPrice[allPrices[i].date.Format("2006-01")] = pf
	}

	t.Logf("Prices loaded: %d", len(prices))
	t.Logf("Month keys from prices: %v", getSortedKeys(monthLastPrice))

	// Dividendos por mês (12 meses)
	dividendByMonth := map[string]float64{
		"2025-02": 0.12, // Fev/2025
		"2025-01": 0.12,
		"2024-12": 0.12,
		"2024-11": 0.12,
		"2024-10": 0.12,
		"2024-09": 0.12,
		"2024-08": 0.12,
		"2024-07": 0.12,
		"2024-06": 0.12,
		"2024-05": 0.12,
		"2024-04": 0.12,
		"2024-03": 0.12,
	}

	// monthKeys vem das cotações, não dos dividendos!
	monthKeys := getSortedKeys(monthLastPrice)
	t.Logf("Month keys from cotations: %v", monthKeys)

	// Verificar quais meses têm dividendos mas não têm preços
	for mk, div := range dividendByMonth {
		if _, ok := monthLastPrice[mk]; !ok {
			t.Logf("MISSING: %s has dividend %.4f but NO price!", mk, div)
		}
	}

	// Como no código real - DY só calculado onde há preço
	dyByMonth := []float64{}
	for _, mk := range monthKeys {
		div := dividendByMonth[mk]
		price := monthLastPrice[mk]
		if div > 0 && price > 0 {
			dy := div / price * 100
			dyByMonth = append(dyByMonth, dy)
			t.Logf("%s: div=%.4f price=%.4f dy=%.4f%% INCLUDED", mk, div, price, dy)
		} else {
			t.Logf("%s: div=%.4f price=%.4f EXCLUDED (no data)", mk, div, price)
		}
	}

	if len(dyByMonth) == 0 {
		t.Fatal("No DY calculated!")
	}

	dyMean := analytics.Mean(dyByMonth)
	t.Logf("DY monthly mean: %.4f%%", dyMean)
	t.Logf("Expected: ~1.4%%")

	// O problema: Fev/2025 tem dividend mas não tem preço!
	// Por isso a média pode estar errada
}

func TestFindMissingPrices(t *testing.T) {
	// O problema pode ser que Feb/2025 não tem cotação no banco

	dividendMonths := []string{
		"2025-02", "2025-01", "2024-12", "2024-11", "2024-10",
		"2024-09", "2024-08", "2024-07", "2024-06", "2024-05",
		"2024-04", "2024-03",
	}

	// Simulando que Feb/2025 não tem preço
	priceMonths := []string{
		"2025-01", "2024-12", "2024-11", "2024-10",
		"2024-09", "2024-08", "2024-07", "2024-06", "2024-05",
		"2024-04", "2024-03", "2024-02",
	}

	priceSet := map[string]bool{}
	for _, m := range priceMonths {
		priceSet[m] = true
	}

	t.Log("Checking for missing prices...")
	for _, m := range dividendMonths {
		if !priceSet[m] {
			t.Logf("MISSING PRICE: %s - dividend exists but no cotation!", m)
		}
	}

	// O problema: 2025-02 está faltando!
	// Isso faz com que esse mês seja ignorado no cálculo do DY
}

func TestDebugMonthKeys(t *testing.T) {
	// Demonstrando o bug: monthKeys vem das cotações, não dos dividendos

	// Cotações disponíveis
	prices := map[string]float64{
		"2025-01": 8.93,
		"2024-12": 8.93,
		"2024-11": 8.93,
		"2024-10": 8.75,
		"2024-09": 8.59,
		"2024-08": 8.68,
		"2024-07": 8.93,
		"2024-06": 9.02,
		"2024-05": 8.76,
		"2024-04": 8.89,
		"2024-03": 9.02,
		"2024-02": 8.50,
	}

	// Dividendos
	dividends := map[string]float64{
		"2025-02": 0.12, // FEV/2025 - sem preço!
		"2025-01": 0.12,
		"2024-12": 0.12,
		"2024-11": 0.12,
		"2024-10": 0.12,
		"2024-09": 0.12,
		"2024-08": 0.12,
		"2024-07": 0.12,
		"2024-06": 0.12,
		"2024-05": 0.12,
		"2024-04": 0.12,
		"2024-03": 0.12,
	}

	// Como no código real
	monthKeys := getSortedKeys(prices)
	t.Logf("monthKeys (from prices): %v", monthKeys)

	// O loop usa monthKeys, não as chaves de dividends!
	var dyByMonth []float64
	for _, mk := range monthKeys {
		div := dividends[mk]
		price := prices[mk]
		if div > 0 && price > 0 {
			dy := div / price * 100
			dyByMonth = append(dyByMonth, dy)
			t.Logf("%s: INCLUDED dy=%.4f%%", mk, dy)
		}
	}

	dyMean := analytics.Mean(dyByMonth)
	t.Logf("\nDY mean from %d months: %.4f%%", len(dyByMonth), dyMean)

	// Expected: (11 meses com preços) / 11 = ~1.4%
	// Mas se Feb/2025 tinha dividend menor (0.05), a média real seria mais baixa!

	// Verificar: Feb/2025 tem dividend 0.12 ou 0.05?
	t.Log("\nDividends by month:")
	for mk, div := range dividends {
		t.Logf("  %s: %.4f", mk, div)
	}
}

func getSortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
