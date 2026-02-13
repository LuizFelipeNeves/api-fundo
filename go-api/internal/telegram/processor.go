package telegram

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type Processor struct {
	Repo   *Repo
	Client *Client
}

func (p *Processor) ProcessUpdate(ctx context.Context, update *model.TelegramUpdate) error {
	if strings.TrimSpace(p.Client.Token) == "" {
		return nil
	}
	if update == nil {
		return nil
	}

	callback := update.CallbackQuery
	callbackData := strings.TrimSpace(func() string {
		if callback == nil {
			return ""
		}
		return callback.Data
	}())

	msg := update.Message
	if msg == nil && callback != nil {
		msg = callback.Message
	}
	if msg == nil {
		return nil
	}

	text := strings.TrimSpace(msg.Text)
	if text == "" && callbackData == "" {
		return nil
	}

	chatIDStr := strconv.FormatInt(msg.Chat.ID, 10)
	if chatIDStr == "" {
		return nil
	}

	if err := p.Repo.UpsertUser(ctx, chatIDStr, msg.Chat.Username, msg.Chat.FirstName, msg.Chat.LastName); err != nil {
		return err
	}

	if callback != nil && strings.TrimSpace(callback.ID) != "" {
		_ = p.Client.AckCallbackQuery(ctx, callback.ID)
	}

	cmd := botCommand{Kind: KindHelp}
	if kind, token, ok := ParseCallback(callbackData); ok {
		switch kind {
		case KindConfirm:
			cmd = botCommand{Kind: KindConfirm, Code: token}
		case KindCancel:
			cmd = botCommand{Kind: KindCancel, Code: token}
		}
	} else {
		cmd = ParseBotCommand(text)
	}

	switch cmd.Kind {
	case KindHelp:
		return p.handleHelp(ctx, chatIDStr)
	case KindList:
		return p.handleList(ctx, chatIDStr)
	case KindSet:
		return p.handleSet(ctx, chatIDStr, cmd.Codes)
	case KindAdd:
		return p.handleAdd(ctx, chatIDStr, cmd.Codes)
	case KindRemove:
		return p.handleRemove(ctx, chatIDStr, cmd.Codes)
	case KindDocumentos:
		return p.handleDocumentos(ctx, chatIDStr, cmd.Code, cmd.Limit)
	case KindCancel:
		return p.handleCancel(ctx, chatIDStr, cmd.Code)
	case KindConfirm:
		return p.handleConfirm(ctx, chatIDStr, cmd.Code)
	default:
		return p.handleHelp(ctx, chatIDStr)
	}
}

func (p *Processor) handleHelp(ctx context.Context, chatID string) error {
	text := strings.TrimSpace(strings.Join([]string{
		"Comandos:",
		"/lista — ver sua lista",
		"/set CODE1 CODE2 ... — substituir sua lista",
		"/add CODE1 CODE2 ... — adicionar fundos",
		"/remove CODE1 CODE2 ... — remover fundos",
		"/documentos [CODE] [LIMITE] — listar documentos recentes",
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
