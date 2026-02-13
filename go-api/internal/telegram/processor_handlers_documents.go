package telegram

import (
	"context"
	"os"
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
			if isHTMLFilenameOrContentType(filename, contentType) {
				if raw, err := os.ReadFile(tmpPath); err == nil {
					extracted := extractHTMLTablePairs(string(raw))
					if len(extracted) > 0 {
						outPath, err := writeTempJSONFile(extracted, "tg-html-extract-*.json")
						if err != nil {
							warnings = append(warnings, "⚠️ "+code+": não consegui processar o documento.")
							return
						}
						defer os.Remove(outPath)

						jsonName := strings.TrimSuffix(filename, filepath.Ext(filename)) + ".json"
						if strings.TrimSpace(jsonName) == ".json" {
							jsonName = strings.ToUpper(strings.TrimSpace(code)) + "-documento.json"
						}
						if err := p.Client.SendDocument(ctx, chatID, outPath, jsonName, caption, "application/json"); err != nil {
							warnings = append(warnings, "⚠️ "+code+": não consegui enviar o documento processado.")
						}
						return
					}
				}
			}

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
