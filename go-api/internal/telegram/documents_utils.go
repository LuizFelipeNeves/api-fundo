package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func isHTMLFilenameOrContentType(filename string, contentType string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	name := strings.ToLower(strings.TrimSpace(filename))
	return strings.Contains(ct, "html") || strings.HasSuffix(name, ".html") || strings.HasSuffix(name, ".htm")
}

func writeTempJSONFile(data any, pattern string) (filePath string, err error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	tmpPath := f.Name()
	defer func() {
		_ = f.Close()
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return tmpPath, nil
}

func downloadToTemp(ctx context.Context, tg *Client, rawURL string, fundCode string, documentID int) (filePath string, filename string, contentType string, err error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", "", fmt.Errorf("invalid url scheme")
	}

	client := http.DefaultClient
	if tg != nil && tg.HTTP != nil {
		client = tg.HTTP
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", "", "", err
	}

	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.9,*/*;q=0.8")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", "", "", fmt.Errorf("download status=%d", resp.StatusCode)
	}

	ct := strings.TrimSpace(resp.Header.Get("content-type"))
	if ct != "" {
		if i := strings.Index(ct, ";"); i >= 0 {
			ct = strings.TrimSpace(ct[:i])
		}
	}
	ctLower := strings.ToLower(ct)

	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		base = fmt.Sprintf("%s-%d", strings.ToUpper(strings.TrimSpace(fundCode)), documentID)
	}
	if filepath.Ext(base) == "" {
		if strings.Contains(ctLower, "pdf") {
			base += ".pdf"
		} else if strings.Contains(ctLower, "html") {
			base += ".html"
		}
	}

	tmp, err := os.CreateTemp("", "tg-doc-*"+filepath.Ext(base))
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()

	const maxBytes = 35 << 20
	if _, err := io.Copy(tmp, io.LimitReader(resp.Body, maxBytes)); err != nil {
		_ = tmp.Close()
		return "", "", "", err
	}
	if err := tmp.Close(); err != nil {
		return "", "", "", err
	}

	if ct == "" {
		ct = "application/octet-stream"
	}
	return tmp.Name(), base, ct, nil
}
