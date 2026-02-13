package telegram

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestExtractHTMLTablePairs_FNET_Real_ExibirDocumento_534301(t *testing.T) {
	if os.Getenv("RUN_FNET_LIVE_TEST") != "1" {
		t.Skip("set RUN_FNET_LIVE_TEST=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := "https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id=534301&cvm=true&"
	tmpPath, filename, contentType, err := downloadToTemp(ctx, nil, url, "TEST", 534301)
	if err != nil {
		t.Fatalf("downloadToTemp: %v", err)
	}
	defer os.Remove(tmpPath)
	t.Logf("downloaded file=%s name=%s contentType=%s", tmpPath, filename, contentType)

	raw, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("read tmp: %v", err)
	}

	if strings.Contains(strings.ToLower(contentType), "pdf") || strings.HasSuffix(strings.ToLower(filename), ".pdf") {
		if len(raw) < 5 || !bytes.HasPrefix(raw, []byte("%PDF")) {
			t.Fatalf("expected a PDF file, got contentType=%q filename=%q", contentType, filename)
		}
		return
	}

	rawStr := string(raw)
	if !strings.Contains(rawStr, "Lei 11.033/2004") {
		t.Fatalf("expected raw html to mention Lei 11.033/2004")
	}
	if !strings.Contains(rawStr, "Lei 11.196/2005") {
		t.Fatalf("expected raw html to mention Lei 11.196/2005")
	}

	extracted := extractHTMLTablePairs(rawStr)
	if len(extracted) == 0 {
		t.Fatalf("expected non-empty extracted map")
	}

	keys := make([]string, 0, len(extracted))
	for k := range extracted {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, k := range keys {
		lines = append(lines, k+" = "+extracted[k])
	}
	t.Logf("extracted pairs (%d):\n%s", len(lines), strings.Join(lines, "\n"))
}
