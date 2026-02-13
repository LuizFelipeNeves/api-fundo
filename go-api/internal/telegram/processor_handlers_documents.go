package telegram

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func pickLimit(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 50 {
		return 50
	}
	return value
}

func (p *Processor) handleDocumentos(ctx context.Context, chatID string, code string, limit int) error {
	lim := pickLimit(limit, 5)
	fundCode := strings.TrimSpace(strings.ToUpper(code))
	if fundCode != "" {
		existing, err := p.Repo.ListExistingFundCodes(ctx, []string{fundCode})
		if err != nil {
			return err
		}
		if len(existing) == 0 {
			return p.Client.SendText(ctx, chatID, "Fundo não encontrado: "+fundCode, nil)
		}
		docs, err := p.Repo.ListLatestDocuments(ctx, []string{fundCode}, lim)
		if err != nil {
			return err
		}
		return p.Client.SendText(ctx, chatID, FormatDocumentsMessage(docs, lim, fundCode), nil)
	}

	funds, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	if len(funds) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}
	docs, err := p.Repo.ListLatestDocuments(ctx, funds, lim)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatDocumentsMessage(docs, lim, ""), nil)
}

func (p *Processor) handleResumoDocumento(ctx context.Context, chatID string, codes []string) error {
	requested := uniqueUppercase(codes)
	if len(requested) == 0 {
		funds, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		requested = uniqueUppercase(funds)
	}
	if len(requested) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}
	if len(requested) > 2 {
		requested = requested[:2]
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, requested)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		return p.Client.SendText(ctx, chatID, "Nenhum fundo encontrado: "+strings.Join(requested, ", "), nil)
	}
	if len(existing) > 2 {
		existing = existing[:2]
	}

	latest, err := p.Repo.ListLatestDocumentsByFund(ctx, existing)
	if err != nil {
		return err
	}

	warnings := []string{}
	for _, code := range existing {
		doc, ok := latest[code]
		if !ok || strings.TrimSpace(doc.URL) == "" {
			warnings = append(warnings, "⚠️ "+code+": sem documentos no banco.")
			continue
		}

		captionLines := []string{
			code,
			strings.TrimSpace(strings.Join(filterEmpty([]string{strings.TrimSpace(doc.Category), strings.TrimSpace(doc.Type)}), " · ")),
			FormatDateHuman(doc.DateUpload),
			strings.TrimSpace(doc.URL),
		}
		caption := clipText(strings.TrimSpace(strings.Join(filterEmpty(captionLines), "\n")), 900)

		tmpPath, filename, contentType, err := downloadToTemp(ctx, p.Client, doc.URL, code, doc.DocumentID)
		if err != nil {
			warnings = append(warnings, "⚠️ "+code+": não consegui baixar o documento.")
			continue
		}
		func() {
			defer os.Remove(tmpPath)
			if err := p.Client.SendDocument(ctx, chatID, tmpPath, filename, caption, contentType); err != nil {
				warnings = append(warnings, "⚠️ "+code+": não consegui enviar o arquivo no Telegram.")
			}
		}()
	}

	if len(warnings) > 0 {
		return p.Client.SendText(ctx, chatID, strings.Join(warnings, "\n"), nil)
	}
	return nil
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

	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		base = fmt.Sprintf("%s-%d", strings.ToUpper(strings.TrimSpace(fundCode)), documentID)
	}
	if filepath.Ext(base) == "" {
		if strings.Contains(ct, "pdf") {
			base += ".pdf"
		} else if strings.Contains(ct, "html") {
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
