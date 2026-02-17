package parsers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NormalizedIndicators represents normalized indicators data
type NormalizedIndicators map[string][]IndicatorData

type IndicatorData struct {
	Year  string   `json:"year"`
	Value *float64 `json:"value"`
}

// NormalizedCotations represents normalized cotations data
type NormalizedCotations struct {
	Real []CotationItem `json:"real"`
}

// CotationItem represents a single cotation entry
type CotationItem struct {
	Date  string  `json:"date"`
	Price float64 `json:"price"`
}

const FNET_BASE = "https://fnet.bmfbovespa.com.br/fnet/publico"

type CotationTodayItem struct {
	Price float64 `json:"price"`
	Hour  string  `json:"hour"`
}

type CotationsTodayData []CotationTodayItem

// DocumentData represents a document entry
type DocumentData struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Category   string `json:"category"`
	Type       string `json:"type"`
	Date       string `json:"date"`
	DateUpload string `json:"dateUpload"`
	URL        string `json:"url"`
	Status     string `json:"status"`
	Version    string `json:"version"`
}

// NormalizeIndicators normalizes indicators JSON response
func NormalizeIndicators(raw map[string][]interface{}) NormalizedIndicators {
	normalized := make(NormalizedIndicators, len(raw))

	for indicatorName, values := range raw {
		normalizedName := indicatorKeyMap[indicatorName]
		if normalizedName == "" {
			normalizedName = indicatorName
		}

		out := make([]IndicatorData, 0, len(values))
		for _, it := range values {
			m, ok := it.(map[string]interface{})
			if !ok || m == nil {
				continue
			}

			year := ""
			if v, ok := m["year"]; ok {
				year = strings.TrimSpace(fmt.Sprint(v))
			}

			out = append(out, IndicatorData{
				Year:  year,
				Value: extractFloat64Ptr(m["value"]),
			})
		}

		normalized[normalizedName] = out
	}

	return normalized
}

func NormalizeIndicatorsAny(raw interface{}) NormalizedIndicators {
	switch v := raw.(type) {
	case map[string][]interface{}:
		return NormalizeIndicators(v)
	case map[string]interface{}:
		converted := make(map[string][]interface{}, len(v))
		for k, value := range v {
			if value == nil {
				converted[k] = []interface{}{}
				continue
			}
			if arr, ok := value.([]interface{}); ok {
				converted[k] = arr
				continue
			}
			converted[k] = []interface{}{}
		}
		return NormalizeIndicators(converted)
	case []interface{}:
		return NormalizedIndicators{}
	default:
		return NormalizedIndicators{}
	}
}

var indicatorKeyMap = map[string]string{
	"COTAS EMITIDAS":           "cotas_emitidas",
	"NÚMERO DE COTISTAS":       "numero_de_cotistas",
	"VACÂNCIA":                 "vacancia",
	"VAL. PATRIMONIAL P/ COTA": "valor_patrimonial_cota",
	"VALOR PATRIMONIAL":        "valor_patrimonial",
	"LIQUIDEZ DIÁRIA":          "liquidez_diaria",
	"DIVIDEND YIELD (DY)":      "dividend_yield",
	"P/VP":                     "pvp",
	"VALOR DE MERCADO":         "valor_mercado",
}

func extractFloat64Ptr(raw interface{}) *float64 {
	if raw == nil {
		return nil
	}

	switch v := raw.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return &v
	case float32:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return &f
	case int:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	case int32:
		f := float64(v)
		return &f
	case uint:
		f := float64(v)
		return &f
	case uint64:
		f := float64(v)
		return &f
	case uint32:
		f := float64(v)
		return &f
	case json.Number:
		f, err := v.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return &f
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil
		}
		s = strings.ReplaceAll(s, ",", ".")
		s = strings.ReplaceAll(s, "%", "")
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return &f
	default:
		s := strings.TrimSpace(fmt.Sprint(raw))
		if s == "" {
			return nil
		}
		s = strings.ReplaceAll(s, ",", ".")
		s = strings.ReplaceAll(s, "%", "")
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return &f
	}
}

// NormalizeCotations normalizes cotations JSON response
func NormalizeCotations(raw map[string][]interface{}) *NormalizedCotations {
	result := &NormalizedCotations{
		Real: []CotationItem{},
	}

	if realData, ok := raw["real"]; ok {
		for _, item := range realData {
			if itemMap, ok := item.(map[string]interface{}); ok {
				cotation := CotationItem{}

				if date, ok := itemMap["date"].(string); ok {
					cotation.Date = date
				}

				if price, ok := itemMap["price"].(float64); ok {
					cotation.Price = price
				}

				result.Real = append(result.Real, cotation)
			}
		}
	}

	return result
}

// NormalizeCotationsToday normalizes today's cotations response
func NormalizeCotationsToday(raw interface{}) CotationsTodayData {
	if raw == nil {
		return CotationsTodayData{}
	}

	if arr, ok := raw.([]interface{}); ok {
		return canonicalizeCotationsToday(normalizeStatusInvestCotationsToday(arr))
	}

	obj, ok := raw.(map[string]interface{})
	if !ok {
		return CotationsTodayData{}
	}

	realRaw, _ := obj["real"]
	realArr, ok := realRaw.([]interface{})
	if !ok || len(realArr) == 0 {
		return CotationsTodayData{}
	}

	out := make([]CotationTodayItem, 0, len(realArr))
	for _, it := range realArr {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		price := extractPriceAny(m)
		if !isFinitePositive(price) {
			continue
		}

		hourValue, ok := m["created_at"]
		if !ok {
			if v, ok := m["hour"]; ok {
				hourValue = v
			} else if v, ok := m["date"]; ok {
				hourValue = v
			} else {
				hourValue = nil
			}
		}

		hour := formatHour(hourValue)
		if hour == "" {
			continue
		}

		out = append(out, CotationTodayItem{Price: price, Hour: hour})
	}

	return canonicalizeCotationsToday(out)
}

var hhmmRe = regexp.MustCompile(`\b(\d{2}):(\d{2})\b`)

func canonicalizeCotationsToday(items CotationsTodayData) CotationsTodayData {
	if len(items) == 0 {
		return CotationsTodayData{}
	}

	byHour := make(map[string]CotationTodayItem, len(items))
	for _, it := range items {
		if !isFinitePositive(it.Price) {
			continue
		}
		hour := formatHour(it.Hour)
		if hour == "" {
			continue
		}
		byHour[hour] = CotationTodayItem{Price: it.Price, Hour: hour}
	}

	out := make([]CotationTodayItem, 0, len(byHour))
	for _, v := range byHour {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hour < out[j].Hour })
	return out
}

func normalizeStatusInvestCotationsToday(raw []interface{}) CotationsTodayData {
	if len(raw) == 0 {
		return CotationsTodayData{}
	}

	var realEntry map[string]interface{}
	for _, it := range raw {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		if v, ok := m["currencyType"].(float64); ok && int(v) == 1 {
			realEntry = m
			break
		}
		if v, ok := m["symbol"].(string); ok && strings.TrimSpace(v) == "R$" {
			realEntry = m
			break
		}
		if v, ok := m["currency"].(string); ok && strings.TrimSpace(v) == "Real brasileiro" {
			realEntry = m
			break
		}
	}
	if realEntry == nil {
		if m, ok := raw[0].(map[string]interface{}); ok {
			realEntry = m
		}
	}
	if realEntry == nil {
		return CotationsTodayData{}
	}

	pricesRaw, _ := realEntry["prices"].([]interface{})
	if len(pricesRaw) == 0 {
		return CotationsTodayData{}
	}

	out := make([]CotationTodayItem, 0, len(pricesRaw))
	for _, it := range pricesRaw {
		mapped, ok := mapStatusInvestPriceItem(it)
		if ok {
			out = append(out, mapped)
		}
	}
	return out
}

func mapStatusInvestPriceItem(item interface{}) (CotationTodayItem, bool) {
	price := extractPriceAny(item)
	if !isFinitePositive(price) {
		return CotationTodayItem{}, false
	}
	hour := formatHour(extractTimeAny(item))
	if hour == "" {
		return CotationTodayItem{}, false
	}
	return CotationTodayItem{Price: price, Hour: hour}, true
}

func extractTimeAny(item interface{}) interface{} {
	m, ok := item.(map[string]interface{})
	if !ok {
		return nil
	}

	if v, ok := m["hour"]; ok {
		return v
	}
	if v, ok := m["created_at"]; ok {
		return v
	}
	if v, ok := m["date"]; ok {
		return v
	}
	return nil
}

func extractPriceAny(item interface{}) float64 {
	switch v := item.(type) {
	case float64:
		return v
	case string:
		f, _ := strconv.ParseFloat(strings.ReplaceAll(v, ",", "."), 64)
		return f
	case map[string]interface{}:
		for _, k := range []string{"price", "value", "last", "close", "cotacao", "preco", "v"} {
			raw, ok := v[k]
			if !ok {
				continue
			}
			switch x := raw.(type) {
			case float64:
				return x
			case string:
				f, _ := strconv.ParseFloat(strings.ReplaceAll(x, ",", "."), 64)
				return f
			}
		}
	}
	return math.NaN()
}

func formatHour(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		m := hhmmRe.FindStringSubmatch(v)
		if len(m) == 3 {
			return m[1] + ":" + m[2]
		}
		return ""
	case float64:
		if !isFinitePositive(v) {
			return ""
		}
		sec := int64(v)
		if v > 1e12 {
			t := time.UnixMilli(sec).UTC()
			return t.Format("15:04")
		}
		if v > 1e9 {
			t := time.Unix(sec, 0).UTC()
			return t.Format("15:04")
		}
		return ""
	default:
		return ""
	}
}

func isFinitePositive(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}

// NormalizeDocuments normalizes documents JSON response
func NormalizeDocuments(raw []interface{}) []DocumentData {
	var documents []DocumentData

	for _, item := range raw {
		if docMap, ok := item.(map[string]interface{}); ok {
			doc := DocumentData{}

			if id, ok := docMap["id"].(string); ok {
				doc.ID = strings.TrimSpace(id)
			} else if idFloat, ok := docMap["id"].(float64); ok {
				doc.ID = strconv.FormatInt(int64(idFloat), 10)
			}

			if title, ok := docMap["descricaoFundo"].(string); ok {
				doc.Title = title
			}

			if category, ok := docMap["categoriaDocumento"].(string); ok {
				doc.Category = category
			}

			if docType, ok := docMap["tipoDocumento"].(string); ok {
				doc.Type = docType
			}

			if date, ok := docMap["dataReferencia"].(string); ok {
				doc.Date = date
			}

			if dateUpload, ok := docMap["dataEntrega"].(string); ok {
				doc.DateUpload = dateUpload
			}

			doc.URL = fmt.Sprintf(
				"%s/exibirDocumento?id=%s&cvm=true&",
				FNET_BASE,
				doc.ID,
			)

			if status, ok := docMap["descricaoStatus"].(string); ok {
				doc.Status = status
			}

			if version, ok := docMap["versao"].(string); ok {
				doc.Version = strings.TrimSpace(version)
			} else if versionFloat, ok := docMap["versao"].(float64); ok {
				doc.Version = strconv.FormatInt(int64(versionFloat), 10)
			}
			if doc.Version == "" {
				doc.Version = "0"
			}

			if doc.ID == "" {
				continue
			}

			documents = append(documents, doc)
		}
	}

	return documents
}

// SHA256Hash calculates SHA256 hash of JSON data
func SHA256Hash(data interface{}) (string, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(jsonBytes)
	return hex.EncodeToString(hash[:]), nil
}

// DividendTypeToCode converts dividend type string to code
func DividendTypeToCode(typeStr string) int {
	switch typeStr {
	case "Dividendos":
		return 1
	case "Amortização":
		return 2
	default:
		return 0
	}
}
