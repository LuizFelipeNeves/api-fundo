package telegram

import (
	"context"
	"strings"
	"time"
)

func (p *Processor) handleHelp(ctx context.Context, chatID string) error {
	text := strings.TrimSpace(strings.Join([]string{
		"Comandos:",
		"/lista — ver sua lista",
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
