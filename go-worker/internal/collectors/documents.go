package collectors

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// DocumentsCollector collects fund documents from FNET
type DocumentsCollector struct {
	fnetClient *httpclient.FnetClient
	db         *db.DB
}

// NewDocumentsCollector creates a new documents collector
func NewDocumentsCollector(fnetClient *httpclient.FnetClient, database *db.DB) *DocumentsCollector {
	return &DocumentsCollector{
		fnetClient: fnetClient,
		db:         database,
	}
}

// Name returns the collector name
func (c *DocumentsCollector) Name() string {
	return "documents"
}

// Collect fetches fund documents from FNET
func (c *DocumentsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	code := parsers.NormalizeFundCode(req.FundCode)
	if verboseLogs() {
		log.Printf("[documents] collecting documents for %s\n", code)
	}

	// Get CNPJ from database
	cnpj, err := c.db.GetFundCNPJByCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to get fund CNPJ: %w", err)
	}

	if cnpj == "" {
		return nil, fmt.Errorf("CNPJ not found for fund: %s", code)
	}

	lastMaxID, err := c.db.GetLastDocumentsMaxIDByCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to get last documents max id: %w", err)
	}

	// Build FNET URLs
	initURL := fmt.Sprintf("%s/abrirGerenciadorDocumentosCVM?cnpjFundo=%s", httpclient.FnetBase, cnpj)
	dataURL := fmt.Sprintf("%s/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&o%%5B0%%5D%%5BdataReferencia%%5D=desc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&isSession=true", httpclient.FnetBase)

	// Fetch documents with session
	var response FnetDocumentsResponse
	err = c.fnetClient.FetchWithSession(ctx, initURL, dataURL, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch documents: %w", err)
	}

	// Normalize documents
	documents := parsers.NormalizeDocuments(response.Data)

	maxFetchedID := 0
	hasMaxFetchedID := false
	for _, doc := range documents {
		id, err := strconv.Atoi(doc.ID)
		if err != nil || id <= 0 {
			continue
		}
		if !hasMaxFetchedID || id > maxFetchedID {
			maxFetchedID = id
			hasMaxFetchedID = true
		}
	}

	if lastMaxID > 0 && hasMaxFetchedID && maxFetchedID <= lastMaxID {
		return &CollectResult{
			Data:      []DocumentItem{},
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}, nil
	}

	// Convert to items with fund code
	var items []DocumentItem
	for _, doc := range documents {
		dateUploadISO := parsers.ToDateISO(doc.DateUpload)
		if dateUploadISO == "" {
			dateUploadISO = parsers.ToDateISO(doc.Date)
		}
		if dateUploadISO == "" {
			dateUploadISO = time.Now().UTC().Format("2006-01-02")
		}

		items = append(items, DocumentItem{
			FundCode:      code,
			DocumentID:    doc.ID,
			Title:         doc.Title,
			Category:      doc.Category,
			Type:          doc.Type,
			Date:          doc.Date,
			DateUploadISO: dateUploadISO,
			DateUpload:    doc.DateUpload,
			URL:           doc.URL,
			Status:        doc.Status,
			Version:       doc.Version,
		})
	}

	return &CollectResult{
		Data:      items,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// FnetDocumentsResponse represents FNET API response
type FnetDocumentsResponse struct {
	Data []interface{} `json:"data"`
}

// DocumentItem represents a document entry
type DocumentItem struct {
	FundCode      string
	DocumentID    string
	Title         string
	Category      string
	Type          string
	Date          string
	DateUploadISO string
	DateUpload    string
	URL           string
	Status        string
	Version       string
}
