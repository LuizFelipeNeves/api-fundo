package config

import (
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                   int
	DatabaseURL            string
	PGPoolMax              int
	LogRequests            bool
	TelegramBotToken       string
	TelegramWebhookToken   string
	APIEndpoint            string
	DocumentNotifyInterval time.Duration
	HTTPClientTimeout      time.Duration
}

func Load(getenv func(string) string) Config {
	cfg := Config{
		Port:                   8080,
		DatabaseURL:            strings.TrimSpace(getenv("DATABASE_URL")),
		PGPoolMax:              2,
		LogRequests:            strings.TrimSpace(getenv("LOG_REQUESTS")) != "0",
		TelegramBotToken:       strings.TrimSpace(getenv("TELEGRAM_BOT_TOKEN")),
		TelegramWebhookToken:   strings.TrimSpace(getenv("TELEGRAM_WEBHOOK_TOKEN")),
		APIEndpoint:            strings.TrimSpace(getenv("API_ENDPOINT")),
		DocumentNotifyInterval: parseInterval(getenv("DOCUMENT_NOTIFY_INTERVAL"), time.Minute),
		HTTPClientTimeout:      15 * time.Second,
	}

	if cfg.APIEndpoint == "" {
		cfg.APIEndpoint = "http://localhost:8080"
	}

	if raw := strings.TrimSpace(getenv("PORT")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n < 65536 {
			cfg.Port = n
		}
	}

	if v := strings.TrimSpace(getenv("PG_POOL_MAX")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 50 {
				n = 50
			}
			cfg.PGPoolMax = n
		}
	}

	return cfg
}

func parseInterval(raw string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(raw)
	if v == "" {
		return fallback
	}
	if v == "0" || v == "0s" {
		return 0
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return fallback
}
