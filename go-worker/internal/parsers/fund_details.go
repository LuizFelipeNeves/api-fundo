package parsers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// FundDetails represents fund details extracted from HTML
type FundDetails struct {
	ID                   string
	CNPJ                 string
	RazaoSocial          string
	PublicoAlvo          string
	Mandato              string
	Segmento             string
	TipoFundo            string
	PrazoDuracao         string
	TipoGestao           string
	TaxaAdministracao    float64
	DailyLiquidity       *float64
	Vacancia             *float64
	NumeroCotistas       *int
	CotasEmitidas        *int64
	ValorPatrimonialCota *float64
	ValorPatrimonial     *float64
	UltimoRendimento     *float64
}

// DividendItem represents a dividend entry from the history table
type DividendItem struct {
	Date    string
	Payment string
	Type    string
	Value   float64
}

// ExtractFundDetails extracts fund details from HTML
func ExtractFundDetails(html, code string) (*FundDetails, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	details := &FundDetails{}

	// Extract fund ID from data attributes or scripts
	details.ID = extractFundID(doc)

	// Extract CNPJ
	doc.Find("._card-body span").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		if strings.Contains(text, "CNPJ") {
			cnpj := extractCNPJ(text)
			if cnpj != "" {
				details.CNPJ = cnpj
			}
		}
	})

	// Extract other details from the info cards
	doc.Find("._card").Each(func(i int, card *goquery.Selection) {
		label := strings.TrimSpace(card.Find("._card-header span").First().Text())
		value := strings.TrimSpace(card.Find("._card-body span").First().Text())

		switch {
		case strings.Contains(label, "Razão Social"):
			details.RazaoSocial = value
		case strings.Contains(label, "Público Alvo"):
			details.PublicoAlvo = value
		case strings.Contains(label, "Mandato"):
			details.Mandato = value
		case strings.Contains(label, "Segmento"):
			details.Segmento = value
		case strings.Contains(label, "Tipo de Fundo"):
			details.TipoFundo = value
		case strings.Contains(label, "Prazo de Duração"):
			details.PrazoDuracao = value
		case strings.Contains(label, "Tipo de Gestão"):
			details.TipoGestao = value
		case strings.Contains(label, "Taxa de Administração"):
			if val := parsePercentage(value); val != nil {
				details.TaxaAdministracao = *val
			}
		case strings.Contains(label, "Liquidez Diária"):
			details.DailyLiquidity = parseCurrency(value)
		case strings.Contains(label, "Vacância"):
			details.Vacancia = parsePercentage(value)
		case strings.Contains(label, "Número de Cotistas"):
			details.NumeroCotistas = parseInt(value)
		case strings.Contains(label, "Cotas Emitidas"):
			details.CotasEmitidas = parseInt64(value)
		case strings.Contains(label, "Valor Patrimonial por Cota"):
			details.ValorPatrimonialCota = parseCurrency(value)
		case strings.Contains(label, "Valor Patrimonial"):
			details.ValorPatrimonial = parseCurrency(value)
		case strings.Contains(label, "Último Rendimento"):
			details.UltimoRendimento = parseCurrency(value)
		}
	})

	return details, nil
}

// ExtractFundID extracts fund ID from HTML
func extractFundID(doc *goquery.Document) string {
	// Try to find ID in data attributes
	if id, exists := doc.Find("[data-fii-id]").Attr("data-fii-id"); exists {
		return id
	}

	// Try to extract from JavaScript variables
	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		script := s.Text()
		re := regexp.MustCompile(`fiiId\s*=\s*['"](\d+)['"]`)
		if matches := re.FindStringSubmatch(script); len(matches) > 1 {
			return
		}
	})

	return ""
}

// ExtractDividendsHistory extracts dividends from the history table
func ExtractDividendsHistory(html string) ([]DividendItem, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var dividends []DividendItem

	// Find the dividends table
	doc.Find("table.table tbody tr").Each(func(i int, row *goquery.Selection) {
		cols := row.Find("td")
		if cols.Length() < 4 {
			return
		}

		date := strings.TrimSpace(cols.Eq(0).Text())
		payment := strings.TrimSpace(cols.Eq(1).Text())
		typeStr := strings.TrimSpace(cols.Eq(2).Text())
		valueStr := strings.TrimSpace(cols.Eq(3).Text())

		if date == "" || payment == "" {
			return
		}

		value := parseCurrencyFloat(valueStr)

		dividends = append(dividends, DividendItem{
			Date:    date,
			Payment: payment,
			Type:    typeStr,
			Value:   value,
		})
	})

	return dividends, nil
}

// Helper functions for parsing

func extractCNPJ(text string) string {
	re := regexp.MustCompile(`\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}`)
	return re.FindString(text)
}

func parsePercentage(s string) *float64 {
	s = strings.ReplaceAll(s, "%", "")
	s = strings.ReplaceAll(s, ",", ".")
	s = strings.TrimSpace(s)

	if val, err := strconv.ParseFloat(s, 64); err == nil {
		return &val
	}
	return nil
}

func parseCurrency(s string) *float64 {
	val := parseCurrencyFloat(s)
	if val == 0 {
		return nil
	}
	return &val
}

func parseCurrencyFloat(s string) float64 {
	// Remove currency symbols and spaces
	s = strings.ReplaceAll(s, "R$", "")
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	s = strings.TrimSpace(s)

	if val, err := strconv.ParseFloat(s, 64); err == nil {
		return val
	}
	return 0
}

func parseInt(s string) *int {
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	if val, err := strconv.Atoi(s); err == nil {
		return &val
	}
	return nil
}

func parseInt64(s string) *int64 {
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	if val, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &val
	}
	return nil
}

// ToDateISO converts Brazilian date format (DD/MM/YYYY) to ISO (YYYY-MM-DD)
func ToDateISO(brDate string) string {
	parts := strings.Split(brDate, "/")
	if len(parts) != 3 {
		return ""
	}

	day := parts[0]
	month := parts[1]
	year := parts[2]

	// Validate
	if len(day) != 2 || len(month) != 2 || len(year) != 4 {
		return ""
	}

	// Parse to validate date
	_, err := time.Parse("02/01/2006", brDate)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%s-%s-%s", year, month, day)
}

// NormalizeFundCode normalizes fund code
func NormalizeFundCode(code string) string {
	code = strings.TrimSpace(code)
	code = strings.ToUpper(code)
	// Remove non-alphanumeric characters
	re := regexp.MustCompile(`[^A-Z0-9]`)
	return re.ReplaceAllString(code, "")
}
