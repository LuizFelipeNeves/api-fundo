package telegram

import (
	"context"
	"strconv"
	"strings"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type Processor struct {
	Repo   *Repo
	Client *Client
	FII    *fii.Service
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
	case KindCategories:
		return p.handleCategories(ctx, chatIDStr)
	case KindExport:
		return p.handleExport(ctx, chatIDStr, cmd.Codes)
	case KindResumoDoc:
		return p.handleResumoDocumento(ctx, chatIDStr, cmd.Codes)
	case KindSet:
		return p.handleSet(ctx, chatIDStr, cmd.Codes)
	case KindAdd:
		return p.handleAdd(ctx, chatIDStr, cmd.Codes)
	case KindRemove:
		return p.handleRemove(ctx, chatIDStr, cmd.Codes)
	case KindDocumentos:
		return p.handleDocumentos(ctx, chatIDStr, cmd.Code, cmd.Limit)
	case KindPesquisa:
		return p.handlePesquisa(ctx, chatIDStr, cmd.Code)
	case KindCotation:
		return p.handleCotation(ctx, chatIDStr, cmd.Code)
	case KindRankHoje:
		return p.handleRankHoje(ctx, chatIDStr, cmd.Codes)
	case KindRankV:
		return p.handleRankV(ctx, chatIDStr)
	case KindCancel:
		return p.handleCancel(ctx, chatIDStr, cmd.Code)
	case KindConfirm:
		return p.handleConfirm(ctx, chatIDStr, cmd.Code)
	default:
		return p.handleHelp(ctx, chatIDStr)
	}
}
