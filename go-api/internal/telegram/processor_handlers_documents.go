package telegram

import (
	"context"
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
