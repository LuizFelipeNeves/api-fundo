package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

func TestFnet_FetchDocuments_Real_CNPJ41081356000184_Title(t *testing.T) {
	if os.Getenv("RUN_FNET_LIVE_TEST") != "1" {
		t.Skip("set RUN_FNET_LIVE_TEST=1 to run")
	}

	expectedTitle := "BRIO MULTIESTRATÉGIA - FUNDO DE INVESTIMENTO IMOBILIÁRIO RESPONSABILIDADE LIMITADA"
	cnpj := "41081356000184"

	cfg := &config.Config{
		HTTPTimeoutMS:    20000,
		HTTPRetryMax:     3,
		HTTPRetryDelayMS: 500,
	}
	c := NewFnetClient(cfg)

	initURL := fmt.Sprintf("%s/abrirGerenciadorDocumentosCVM?cnpjFundo=%s", FnetBase, cnpj)
	dataURL := fmt.Sprintf("%s/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&o%%5B0%%5D%%5BdataReferencia%%5D=desc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&isSession=true", FnetBase)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp struct {
		Data []any `json:"data"`
	}
	if err := c.FetchWithSession(ctx, initURL, dataURL, &resp); err != nil {
		t.Fatalf("FetchWithSession failed: %v", err)
	}

	if b, err := json.MarshalIndent(resp, "", "  "); err == nil {
		t.Logf("FNET raw response: %s", string(b))
	} else {
		t.Logf("FNET raw response marshal error: %v", err)
	}

	docs := parsers.NormalizeDocuments(resp.Data)
	if len(docs) == 0 {
		t.Fatalf("expected at least one document")
	}

	for _, d := range docs {
		if d.Title == expectedTitle {
			return
		}
	}

	sample := make([]string, 0, 10)
	for i := 0; i < len(docs) && i < 10; i++ {
		sample = append(sample, docs[i].Title)
	}
	t.Fatalf("expected to find title %q; got titles: %q", expectedTitle, sample)
}

func TestFnet_FetchDocuments_Real_CNPJ63194165000161_Title(t *testing.T) {
	if os.Getenv("RUN_FNET_LIVE_TEST") != "1" {
		t.Skip("set RUN_FNET_LIVE_TEST=1 to run")
	}

	expectedTitle := "CIBRA OCP FIAGRO DIREITOS CREDITÓRIOS COMERCIAIS RESPONSABILIDADE LIMITADA"
	cnpj := "63194165000161"

	cfg := &config.Config{
		HTTPTimeoutMS:    20000,
		HTTPRetryMax:     3,
		HTTPRetryDelayMS: 500,
	}
	c := NewFnetClient(cfg)

	initURL := fmt.Sprintf("%s/abrirGerenciadorDocumentosCVM?cnpjFundo=%s", FnetBase, cnpj)
	dataURL := fmt.Sprintf("%s/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&o%%5B0%%5D%%5BdataReferencia%%5D=desc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&isSession=true", FnetBase)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp struct {
		Data []any `json:"data"`
	}
	if err := c.FetchWithSession(ctx, initURL, dataURL, &resp); err != nil {
		t.Fatalf("FetchWithSession failed: %v", err)
	}

	if b, err := json.MarshalIndent(resp, "", "  "); err == nil {
		t.Logf("FNET raw response: %s", string(b))
	} else {
		t.Logf("FNET raw response marshal error: %v", err)
	}

	docs := parsers.NormalizeDocuments(resp.Data)
	if len(docs) == 0 {
		t.Fatalf("expected at least one document")
	}

	for _, d := range docs {
		if d.Title == expectedTitle {
			return
		}
	}

	sample := make([]string, 0, 10)
	for i := 0; i < len(docs) && i < 10; i++ {
		sample = append(sample, docs[i].Title)
	}
	t.Fatalf("expected to find title %q; got titles: %q", expectedTitle, sample)
}
