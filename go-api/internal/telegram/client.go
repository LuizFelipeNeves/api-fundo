package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
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

func (c *Client) SendDocument(ctx context.Context, chatID string, filePath string, filename string, caption string, contentType string) error {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN is empty")
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return fmt.Errorf("chat_id is empty")
	}
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return fmt.Errorf("file_path is empty")
	}
	if strings.TrimSpace(filename) == "" {
		filename = filepath.Base(filePath)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	_ = w.WriteField("chat_id", chatID)
	if strings.TrimSpace(caption) != "" {
		_ = w.WriteField("caption", caption)
	}

	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="document"; filename="%s"`, filename))
	if strings.TrimSpace(contentType) != "" {
		h.Set("Content-Type", contentType)
	}
	part, err := w.CreatePart(h)
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, f); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", token), bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", w.FormDataContentType())

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram sendDocument request failed: %s", redactTelegramToken(err.Error(), token))
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		return fmt.Errorf("telegram sendDocument status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
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

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram sendMessage request failed: %s", redactTelegramToken(err.Error(), token))
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

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram answerCallbackQuery request failed: %s", redactTelegramToken(err.Error(), token))
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		return fmt.Errorf("telegram answerCallbackQuery status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func redactTelegramToken(s string, token string) string {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(s) == "" {
		return s
	}
	return strings.ReplaceAll(s, token, "<redacted>")
}
