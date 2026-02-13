package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	Token string
	HTTP  *http.Client
}

type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type ReplyMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard,omitempty"`
}

func (c *Client) SendMessage(ctx context.Context, chatID string, text string) error {
	return c.SendText(ctx, chatID, text, nil)
}

func (c *Client) SendText(ctx context.Context, chatID string, text string, replyMarkup *ReplyMarkup) error {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN is empty")
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return fmt.Errorf("chat_id is empty")
	}

	type reqBody struct {
		ChatID                string       `json:"chat_id"`
		Text                  string       `json:"text"`
		DisableWebPagePreview bool         `json:"disable_web_page_preview"`
		ReplyMarkup           *ReplyMarkup `json:"reply_markup,omitempty"`
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(reqBody{
		ChatID:                chatID,
		Text:                  text,
		DisableWebPagePreview: true,
		ReplyMarkup:           replyMarkup,
	}); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token), bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func (c *Client) AckCallbackQuery(ctx context.Context, callbackQueryID string) error {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN is empty")
	}
	callbackQueryID = strings.TrimSpace(callbackQueryID)
	if callbackQueryID == "" {
		return nil
	}

	type reqBody struct {
		CallbackQueryID string `json:"callback_query_id"`
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(reqBody{CallbackQueryID: callbackQueryID}); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://api.telegram.org/bot%s/answerCallbackQuery", token), bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		return fmt.Errorf("telegram answerCallbackQuery status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}
