package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/docnotify"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/httpapi"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/telegram"
)

func main() {
	cfg := config.Load(os.Getenv)

	ctx := context.Background()
	conn, err := db.Open(ctx, cfg.DatabaseURL, cfg.PGPoolMax)
	if err != nil {
		log.Fatalf("db error: %v", err)
	}
	defer conn.Close()

	httpClient := &http.Client{
		Timeout: cfg.HTTPClientTimeout,
	}

	tgClient := &telegram.Client{Token: cfg.TelegramBotToken, HTTP: httpClient}
	tgRepo := telegram.NewRepo(conn)
	fiiSvc := fii.New(conn)
	tgProcessor := &telegram.Processor{Repo: tgRepo, Client: tgClient, FII: fiiSvc}

	appCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	notifier := &docnotify.Notifier{
		DB:        conn,
		Telegram:  tgClient,
		FormatMsg: telegram.FormatNewDocumentMessage,
	}
	notifier.Start(appCtx, cfg.DocumentNotifyInterval)

	rt := &httpapi.Router{
		FII:                   fiiSvc,
		Telegram:              tgProcessor,
		TelegramWebhookSecret: cfg.TelegramWebhookSecret,
		TelegramWebhookToken:  cfg.TelegramWebhookToken,
		LogRequests:           cfg.LogRequests,
	}

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           rt.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("server_start port=%d\n", cfg.Port)
	if t := strings.TrimSpace(cfg.TelegramWebhookToken); t != "" {
		base := strings.TrimRight(strings.TrimSpace(cfg.APIEndpoint), "/")
		log.Printf("telegram_webhook_example_url=%s/api/telegram/webhook/%s\n", base, t)
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http error: %v", err)
	}
}
