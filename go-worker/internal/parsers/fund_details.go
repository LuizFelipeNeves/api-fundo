package parsers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var (
	reFirstNumber = regexp.MustCompile(`[-+]?\d+(?:[.,]\d+)?`)
	reDigits      = regexp.MustCompile(`\d+`)
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
	details.ID = extractFundID(html, doc)

	if cnpj := extractCNPJ(html); cnpj != "" {
		details.CNPJ = cnpj
	}

	extractFundDetailsFromDescBlocks(doc, details)

	return details, nil
}

// ExtractFundID extracts fund ID from HTML
func extractFundID(html string, doc *goquery.Document) string {
	if doc != nil {
		if id, exists := doc.Find("[data-fii-id]").Attr("data-fii-id"); exists {
			return strings.TrimSpace(id)
		}
	}

	type pat struct {
		re *regexp.Regexp
	}

	patterns := []pat{
		{re: regexp.MustCompile(`fiiId\s*=\s*['"](\d+)['"]`)},
		{re: regexp.MustCompile(`"fii_id"\s*:\s*(\d+)`)},
		{re: regexp.MustCompile(`api/fii/[^"' ]+/(\d+)`)},
		{re: regexp.MustCompile(`\bfiis/(\d+)\b`)},
	}

	for _, p := range patterns {
		if m := p.re.FindStringSubmatch(html); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}

	return ""
}

func extractFundDetailsFromDescBlocks(doc *goquery.Document, details *FundDetails) {
	if doc == nil || details == nil {
		return
	}

	doc.Find(".desc").Each(func(i int, desc *goquery.Selection) {
		label := strings.TrimSpace(desc.Find(".name").First().Text())
		value := strings.TrimSpace(desc.Find(".value span").First().Text())
		if label == "" || value == "" {
			return
		}

		labelNorm := normalizeLabel(label)

		switch {
		case strings.Contains(labelNorm, "razao social"):
			details.RazaoSocial = value
		case strings.Contains(labelNorm, "cnpj"):
			if cnpj := extractCNPJ(value); cnpj != "" {
				details.CNPJ = cnpj
			}
		case strings.Contains(labelNorm, "publico") && strings.Contains(labelNorm, "alvo"):
			details.PublicoAlvo = value
		case strings.Contains(labelNorm, "mandato"):
			details.Mandato = value
		case strings.Contains(labelNorm, "segmento"):
			details.Segmento = value
		case strings.Contains(labelNorm, "tipo de fundo"):
			details.TipoFundo = value
		case strings.Contains(labelNorm, "prazo") && strings.Contains(labelNorm, "duracao"):
			details.PrazoDuracao = value
		case strings.Contains(labelNorm, "tipo de gestao"):
			details.TipoGestao = value
		case strings.Contains(labelNorm, "taxa") && strings.Contains(labelNorm, "administracao"):
			if val := parsePercentage(value); val != nil {
				details.TaxaAdministracao = *val
			}
		case strings.Contains(labelNorm, "liquidez") && strings.Contains(labelNorm, "diaria"):
			details.DailyLiquidity = parseCurrency(value)
		case strings.Contains(labelNorm, "vacancia"):
			details.Vacancia = parsePercentage(value)
		case strings.Contains(labelNorm, "numero") && strings.Contains(labelNorm, "cotistas"):
			details.NumeroCotistas = parseInt(value)
		case strings.Contains(labelNorm, "cotas") && strings.Contains(labelNorm, "emitidas"):
			details.CotasEmitidas = parseInt64(value)
		case strings.Contains(labelNorm, "patrimonial") && strings.Contains(labelNorm, "cota"):
			details.ValorPatrimonialCota = parseCurrency(value)
		case strings.Contains(labelNorm, "valor patrimonial"):
			details.ValorPatrimonial = parseCurrency(value)
		case strings.Contains(labelNorm, "ultimo") && strings.Contains(labelNorm, "rendimento"):
			details.UltimoRendimento = parseCurrency(value)
		}
	})
}

func normalizeLabel(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)

	repl := strings.NewReplacer(
		"á", "a", "à", "a", "ã", "a", "â", "a", "ä", "a",
		"é", "e", "è", "e", "ê", "e", "ë", "e",
		"í", "i", "ì", "i", "î", "i", "ï", "i",
		"ó", "o", "ò", "o", "õ", "o", "ô", "o", "ö", "o",
		"ú", "u", "ù", "u", "û", "u", "ü", "u",
		"ç", "c",
		"ñ", "n",
		"-", " ",
	)

	s = repl.Replace(s)
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

// ExtractDividendsHistory extracts dividends from the history table
func ExtractDividendsHistory(html string) ([]DividendItem, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var dividends []DividendItem

	table := doc.Find("table#table-dividends-history").First()
	if table.Length() > 0 {
		typeIdx := -1
		dateIdx := -1
		paymentIdx := -1
		valueIdx := -1

		table.Find("thead tr").First().Find("th").Each(func(i int, th *goquery.Selection) {
			h := normalizeLabel(strings.TrimSpace(th.Text()))
			switch {
			case strings.Contains(h, "tipo"):
				typeIdx = i
			case strings.Contains(h, "data") && strings.Contains(h, "com"):
				dateIdx = i
			case strings.Contains(h, "pagamento"):
				paymentIdx = i
			case strings.Contains(h, "valor"):
				valueIdx = i
			}
		})

		if typeIdx < 0 {
			typeIdx = 0
		}
		if dateIdx < 0 {
			dateIdx = 1
		}
		if paymentIdx < 0 {
			paymentIdx = 2
		}
		if valueIdx < 0 {
			valueIdx = 3
		}

		table.Find("tbody tr").Each(func(i int, row *goquery.Selection) {
			cols := row.Find("td")
			if cols.Length() < 4 {
				return
			}

			typeStr := strings.TrimSpace(cols.Eq(typeIdx).Text())
			date := strings.TrimSpace(cols.Eq(dateIdx).Text())
			payment := strings.TrimSpace(cols.Eq(paymentIdx).Text())
			valueStr := strings.TrimSpace(cols.Eq(valueIdx).Text())

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
	} else {
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
	}

	return dividends, nil
}

// Helper functions for parsing

func extractCNPJ(text string) string {
	re := regexp.MustCompile(`\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}`)
	return re.FindString(text)
}

func parsePercentage(s string) *float64 {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.ReplaceAll(s, "%", "")
	s = strings.TrimSpace(s)

	if m := reFirstNumber.FindString(s); m != "" {
		s = m
	}

	s = strings.ReplaceAll(s, ",", ".")

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
	s = strings.ReplaceAll(s, "\u00a0", " ")

	if m := reFirstNumber.FindString(s); m != "" {
		s = m
	}

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
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.Join(reDigits.FindAllString(s, -1), "")
	s = strings.TrimSpace(s)

	if val, err := strconv.Atoi(s); err == nil {
		return &val
	}
	return nil
}

func parseInt64(s string) *int64 {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.Join(reDigits.FindAllString(s, -1), "")
	s = strings.TrimSpace(s)

	if val, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &val
	}
	return nil
}

// ToDateISO converts Brazilian date format (DD/MM/YYYY) to ISO (YYYY-MM-DD)
func ToDateISO(brDate string) string {
	s := strings.TrimSpace(brDate)
	if s == "" {
		return ""
	}
	if len(s) >= 10 {
		head := s[:10]
		if len(head) == 10 && head[2] == '/' && head[5] == '/' {
			s = head
		} else if len(head) == 10 && head[4] == '-' && head[7] == '-' {
			if _, err := time.Parse("2006-01-02", head); err == nil {
				return head
			}
		}
	}

	parts := strings.Split(s, "/")
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
	_, err := time.Parse("02/01/2006", s)
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
