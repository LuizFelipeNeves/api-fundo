package parsers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// NormalizedIndicators represents normalized indicators data
type NormalizedIndicators map[string][]interface{}

// NormalizedCotations represents normalized cotations data
type NormalizedCotations struct {
	Real []CotationItem `json:"real"`
}

// CotationItem represents a single cotation entry
type CotationItem struct {
	Date  string  `json:"date"`
	Price float64 `json:"price"`
}

// CotationsTodayData represents today's cotation data
type CotationsTodayData map[string]interface{}

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
	return NormalizedIndicators(raw)
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
func NormalizeCotationsToday(raw map[string]interface{}) CotationsTodayData {
	return CotationsTodayData(raw)
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

			if title, ok := docMap["title"].(string); ok {
				doc.Title = title
			}

			if category, ok := docMap["category"].(string); ok {
				doc.Category = category
			}

			if docType, ok := docMap["type"].(string); ok {
				doc.Type = docType
			}

			if date, ok := docMap["date"].(string); ok {
				doc.Date = date
			}

			if dateUpload, ok := docMap["dateUpload"].(string); ok {
				doc.DateUpload = dateUpload
			}

			if url, ok := docMap["url"].(string); ok {
				doc.URL = url
			}

			if status, ok := docMap["status"].(string); ok {
				doc.Status = status
			}

			if version, ok := docMap["version"].(string); ok {
				doc.Version = strings.TrimSpace(version)
			} else if versionFloat, ok := docMap["version"].(float64); ok {
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
