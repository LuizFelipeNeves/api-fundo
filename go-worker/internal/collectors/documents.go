package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DocumentsCollector collects fund documents
type DocumentsCollector struct {
	client *http.Client
}

// NewDocumentsCollector creates a new documents collector
func NewDocumentsCollector() *DocumentsCollector {
	return &DocumentsCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *DocumentsCollector) Name() string {
	return "documents"
}

// Document represents a fund document
type Document struct {
	DocumentID    int    `json:"document_id"`
	Title         string `json:"title"`
	Category      string `json:"category"`
	Type          string `json:"type"`
	Date          string `json:"date"`
	DateUploadISO string `json:"date_upload_iso"`
	DateUpload    string `json:"dateUpload"`
	URL           string `json:"url"`
	Status        string `json:"status"`
	Version       int    `json:"version"`
}

// Collect fetches fund documents from the API
func (c *DocumentsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	if req.CNPJ == "" {
		return nil, fmt.Errorf("cnpj is required")
	}

	// Note: The actual API endpoint may differ - this is a placeholder
	// The Node.js implementation may use a different endpoint or service
	url := fmt.Sprintf("https://investidor10.com.br/api/fundos-imobiliarios/%s/documents?cnpj=%s", req.FundCode, req.CNPJ)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch documents: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var documents []Document
	if err := json.NewDecoder(resp.Body).Decode(&documents); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &CollectResult{
		FundCode:  req.FundCode,
		Data:      documents,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
