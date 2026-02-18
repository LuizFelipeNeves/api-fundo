package telegram

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
)

func (p *Processor) handleHelp(ctx context.Context, chatID string) error {
	text := strings.TrimSpace(strings.Join([]string{
		"Comandos:",
		"/lista — ver sua lista",
		"/categorias — agrupar sua lista por categoria",
		"/export [CODE1 CODE2 ...] — exportar JSON agregado",
		"/pesquisa CODE — detalhes do fundo",
		"/cotation CODE — estatísticas de cotação",
		"/resumo-documento [CODE1 CODE2] — enviar último documento (máx 2)",
		"/set CODE1 CODE2 ... — substituir sua lista",
		"/add CODE1 CODE2 ... — adicionar fundos",
		"/remove CODE1 CODE2 ... — remover fundos",
		"/documentos [CODE] [LIMITE] — listar documentos recentes",
		"/rank hoje [CODE1 CODE2 ...] — rank para sua lista (ou codes)",
		"/rankv [CODE1 CODE2 ...] — rank value (ou todos os fundos)",
	}, "\n"))
	return p.Client.SendText(ctx, chatID, text, nil)
}

func (p *Processor) handleList(ctx context.Context, chatID string) error {
	funds, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatFundsListMessage(funds), nil)
}

func (p *Processor) handleCategories(ctx context.Context, chatID string) error {
	funds, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	if len(funds) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}
	info, err := p.Repo.ListFundCategoryInfoByCodes(ctx, funds)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatCategoriesMessage(funds, info), nil)
}

func (p *Processor) handlePesquisa(ctx context.Context, chatID string, code string) error {
	fundCode := strings.ToUpper(strings.TrimSpace(code))
	if fundCode == "" {
		return p.Client.SendText(ctx, chatID, "Envie: /pesquisa CODE", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, []string{fundCode})
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		return p.Client.SendText(ctx, chatID, "Fundo não encontrado: "+fundCode, nil)
	}

	info, err := p.Repo.GetFundPesquisaInfo(ctx, fundCode)
	if err != nil {
		return err
	}
	if info == nil {
		return p.Client.SendText(ctx, chatID, "Fundo não encontrado: "+fundCode, nil)
	}
	return p.Client.SendText(ctx, chatID, FormatPesquisaMessage(*info), nil)
}

func (p *Processor) handleCotation(ctx context.Context, chatID string, code string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

	fundCode := strings.ToUpper(strings.TrimSpace(code))
	if fundCode == "" {
		return p.Client.SendText(ctx, chatID, "Envie: /cotation CODE", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, []string{fundCode})
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		return p.Client.SendText(ctx, chatID, "Fundo não encontrado: "+fundCode, nil)
	}

	stats, ok, err := p.FII.GetCotationStats(ctx, fundCode)
	if err != nil {
		return err
	}
	if !ok || stats == nil {
		return p.Client.SendText(ctx, chatID, "Sem cotações históricas para "+fundCode+".", nil)
	}

	msg := FormatCotationMessage(
		fundCode,
		stats.AsOfISO,
		stats.LastPrice,
		stats.Ret7,
		stats.Ret30,
		stats.Ret90,
		stats.DrawdownMax,
		stats.VolAnnual30d,
		stats.VolAnnual90d,
	)
	return p.Client.SendText(ctx, chatID, msg, nil)
}

func (p *Processor) handleExport(ctx context.Context, chatID string, codes []string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

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

	existing, err := p.Repo.ListExistingFundCodes(ctx, requested)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(requested), existing)

	type fundItem struct {
		Code  string          `json:"code"`
		Ok    bool            `json:"ok"`
		Data  json.RawMessage `json:"data,omitempty"`
		Error string          `json:"error,omitempty"`
	}

	fundsOut := make([]fundItem, 0, len(existing))
	for _, code := range existing {
		exp, found, err := p.FII.ExportFund(ctx, code, fii.ExportFundOptions{})
		if err != nil {
			fundsOut = append(fundsOut, fundItem{Code: code, Ok: false, Error: "internal_error"})
			continue
		}
		if !found || exp == nil {
			fundsOut = append(fundsOut, fundItem{Code: code, Ok: false, Error: "FII não encontrado"})
			continue
		}
		b, err := json.Marshal(exp)
		if err != nil {
			fundsOut = append(fundsOut, fundItem{Code: code, Ok: false, Error: "marshal_error"})
			continue
		}
		fundsOut = append(fundsOut, fundItem{Code: code, Ok: true, Data: b})
	}

	payload := map[string]any{
		"generated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"source":          "telegram",
		"chat_id":         chatID,
		"requested_codes": requested,
		"exported_codes":  existing,
		"missing_codes":   missing,
		"funds":           fundsOut,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	f, err := os.CreateTemp("", "fii-export-"+chatID+"-*.json")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	generatedAt, _ := payload["generated_at"].(string)
	caption := FormatExportMessage(generatedAt, existing, missing)
	return p.Client.SendDocument(ctx, chatID, tmpPath, "fii-export-"+chatID+".json", caption, "application/json")
}

func (p *Processor) handleSet(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /set CODE1 CODE2 ...", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)

	before, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}

	removed := diffStrings(before, existing)
	added := diffStrings(existing, before)

	if len(removed) > 0 {
		createdAt, err := p.Repo.UpsertPendingAction(ctx, chatID, PendingAction{Kind: PendingKindSet, Codes: uppercaseAll(codes)})
		if err != nil {
			return err
		}
		msg := FormatConfirmSetMessage(len(before), existing, added, removed, missing)
		rm := &ReplyMarkup{
			InlineKeyboard: [][]InlineKeyboardButton{{
				{Text: "✅ Confirmar", CallbackData: "confirm:" + createdAt},
				{Text: "❌ Cancelar", CallbackData: "cancel:" + createdAt},
			}},
		}
		return p.Client.SendText(ctx, chatID, msg, rm)
	}

	if err := p.Repo.SetUserFunds(ctx, chatID, existing); err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatSetMessage(existing, added, removed, missing), nil)
}

func (p *Processor) handleAdd(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /add CODE1 CODE2 ...", nil)
	}
	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)
	addedCount, err := p.Repo.AddUserFunds(ctx, chatID, existing)
	if err != nil {
		return err
	}
	nowList, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatAddMessage(addedCount, nowList, missing), nil)
}

func (p *Processor) handleRemove(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /remove CODE1 CODE2 ...", nil)
	}
	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)

	before, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}

	beforeSet := map[string]struct{}{}
	for _, c := range before {
		beforeSet[c] = struct{}{}
	}
	var toRemove []string
	for _, c := range existing {
		if _, ok := beforeSet[c]; ok {
			toRemove = append(toRemove, c)
		}
	}

	if len(toRemove) > 0 {
		createdAt, err := p.Repo.UpsertPendingAction(ctx, chatID, PendingAction{Kind: PendingKindRemove, Codes: uppercaseAll(codes)})
		if err != nil {
			return err
		}
		msg := FormatConfirmRemoveMessage(len(before), toRemove, missing)
		rm := &ReplyMarkup{
			InlineKeyboard: [][]InlineKeyboardButton{{
				{Text: "✅ Confirmar", CallbackData: "confirm:" + createdAt},
				{Text: "❌ Cancelar", CallbackData: "cancel:" + createdAt},
			}},
		}
		return p.Client.SendText(ctx, chatID, msg, rm)
	}

	return p.Client.SendText(ctx, chatID, FormatRemoveMessage(0, before, missing), nil)
}

func (p *Processor) handleCancel(ctx context.Context, chatID string, callbackToken string) error {
	pending, err := p.Repo.GetPendingAction(ctx, chatID)
	if err != nil {
		return err
	}
	if pending == nil {
		return p.Client.SendText(ctx, chatID, "Não há nenhuma ação pendente para cancelar.", nil)
	}
	if callbackToken != "" && pending.CreatedAt.Format(time.RFC3339Nano) != callbackToken && pending.CreatedAt.Format(time.RFC3339) != callbackToken {
		return p.Client.SendText(ctx, chatID, "Esse cancelamento não é mais válido. Envie o comando novamente.", nil)
	}
	if time.Since(pending.CreatedAt) > 10*time.Minute {
		_ = p.Repo.ClearPendingAction(ctx, chatID)
		return p.Client.SendText(ctx, chatID, "Esse cancelamento expirou. Envie o comando novamente.", nil)
	}
	if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, "❌ Cancelado.", nil)
}

func (p *Processor) handleConfirm(ctx context.Context, chatID string, callbackToken string) error {
	pending, err := p.Repo.GetPendingAction(ctx, chatID)
	if err != nil {
		return err
	}
	if pending == nil {
		return p.Client.SendText(ctx, chatID, "Não há nenhuma ação pendente para confirmar.", nil)
	}
	if callbackToken != "" && pending.CreatedAt.Format(time.RFC3339Nano) != callbackToken && pending.CreatedAt.Format(time.RFC3339) != callbackToken {
		return p.Client.SendText(ctx, chatID, "Essa confirmação não é mais válida. Envie o comando novamente.", nil)
	}
	if time.Since(pending.CreatedAt) > 10*time.Minute {
		_ = p.Repo.ClearPendingAction(ctx, chatID)
		return p.Client.SendText(ctx, chatID, "Essa confirmação expirou. Envie o comando novamente.", nil)
	}

	switch pending.Action.Kind {
	case PendingKindSet:
		existing, err := p.Repo.ListExistingFundCodes(ctx, pending.Action.Codes)
		if err != nil {
			return err
		}
		missing := diffStrings(uppercaseAll(pending.Action.Codes), existing)
		before, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		if err := p.Repo.SetUserFunds(ctx, chatID, existing); err != nil {
			return err
		}
		if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
			return err
		}
		removed := diffStrings(before, existing)
		added := diffStrings(existing, before)
		return p.Client.SendText(ctx, chatID, "✅ Confirmado\n\n"+FormatSetMessage(existing, added, removed, missing), nil)
	case PendingKindRemove:
		existing, err := p.Repo.ListExistingFundCodes(ctx, pending.Action.Codes)
		if err != nil {
			return err
		}
		missing := diffStrings(uppercaseAll(pending.Action.Codes), existing)
		removedCount, err := p.Repo.RemoveUserFunds(ctx, chatID, existing)
		if err != nil {
			return err
		}
		if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
			return err
		}
		nowList, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		return p.Client.SendText(ctx, chatID, "✅ Confirmado\n\n"+FormatRemoveMessage(removedCount, nowList, missing), nil)
	}

	_ = p.Repo.ClearPendingAction(ctx, chatID)
	return p.Client.SendText(ctx, chatID, "Não consegui interpretar a ação pendente. Envie o comando novamente.", nil)
}
