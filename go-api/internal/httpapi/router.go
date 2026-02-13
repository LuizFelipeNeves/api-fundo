package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/telegram"
)

type Router struct {
	FII                  *fii.Service
	Telegram             *telegram.Processor
	TelegramWebhookToken string
	LogRequests          bool
}

func (rt *Router) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/docs/", http.StatusTemporaryRedirect)
	})

	mux.HandleFunc("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		if !allowOnlyGet(w, r) {
			return
		}
		writeJSON(w, 200, openapiSpec())
	})

	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/docs/", http.StatusTemporaryRedirect)
	})

	mux.HandleFunc("/docs/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs/" {
			http.NotFound(w, r)
			return
		}
		if !allowOnlyGet(w, r) {
			return
		}
		w.Header().Set("content-type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerUIHTML("/openapi.json")))
	})

	mux.HandleFunc("/api/telegram/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}

		if strings.TrimSpace(rt.TelegramWebhookToken) != "" {
			writeJSON(w, 200, map[string]any{"ok": true})
			return
		}

		rt.processTelegramWebhook(w, r)
	})

	mux.HandleFunc("/api/telegram/webhook/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}

		requiredToken := strings.TrimSpace(rt.TelegramWebhookToken)
		if requiredToken != "" {
			raw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/telegram/webhook/"), "/")
			if raw == "" || strings.Contains(raw, "/") || raw != requiredToken {
				writeJSON(w, 200, map[string]any{"ok": true})
				return
			}
		}

		rt.processTelegramWebhook(w, r)
	})

	mux.HandleFunc("/api/fii/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if rt.FII == nil {
			writeJSON(w, 500, map[string]any{"error": "internal_error"})
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/api/fii/")
		path = strings.Trim(path, "/")

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if path == "" {
			data, err := rt.FII.ListFunds(ctx)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		}

		parts := strings.Split(path, "/")
		codeRaw := parts[0]
		code, ok := fii.ValidateFundCode(codeRaw)
		if !ok {
			writeJSON(w, 400, map[string]any{
				"error":   "Código inválido",
				"message": "Código deve ter formato XXXX11 (4 letras + 11)",
				"example": "binc11",
			})
			return
		}

		if len(parts) == 1 {
			data, err := rt.FII.GetFundDetails(ctx, code)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if data == nil {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		}

		switch parts[1] {
		case "indicators":
			data, found, err := rt.FII.GetLatestIndicators(ctx, code)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if !found {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		case "cotations":
			days, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("days")))
			data, err := rt.FII.GetCotations(ctx, code, days)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if data == nil {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		case "dividends":
			data, found, err := rt.FII.GetDividends(ctx, code)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if !found {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		case "cotations-today":
			data, found, err := rt.FII.GetLatestCotationsToday(ctx, code)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if !found {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		case "documents":
			data, found, err := rt.FII.GetDocuments(ctx, code)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if !found {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}
			writeJSON(w, 200, map[string]any{"data": data})
			return
		case "export":
			cotDays, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("cotationsDays")))
			snapLimit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("indicatorsSnapshotsLimit")))

			data, found, err := rt.FII.ExportFund(ctx, code, fii.ExportFundOptions{
				CotationsDays:            cotDays,
				IndicatorsSnapshotsLimit: snapLimit,
			})
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": "internal_error"})
				return
			}
			if !found || data == nil {
				writeJSON(w, 404, map[string]any{"error": "FII não encontrado"})
				return
			}

			writeJSON(w, 200, data)
			return
		}

		http.NotFound(w, r)
	})

	if !rt.LogRequests {
		return mux
	}

	return withRequestLogging(mux)
}

func (rt *Router) processTelegramWebhook(w http.ResponseWriter, r *http.Request) {
	if rt.Telegram == nil || rt.Telegram.Client == nil || strings.TrimSpace(rt.Telegram.Client.Token) == "" {
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}

	lim := http.MaxBytesReader(w, r.Body, 2<<20)
	defer lim.Close()

	var update model.TelegramUpdate
	dec := json.NewDecoder(lim)
	if err := dec.Decode(&update); err != nil {
		writeJSON(w, 200, map[string]any{"ok": true})
		return
	}

	go func(u model.TelegramUpdate) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := rt.Telegram.ProcessUpdate(ctx, &u); err != nil {
			log.Printf("[telegram_webhook] process error: %v\n", err)
		}
	}(update)

	writeJSON(w, 200, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func withRequestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &statusCapturingResponseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		dur := time.Since(started)

		meta := fmt.Sprintf("method=%s path=%s status=%d duration_ms=%d", r.Method, r.URL.Path, rw.status, dur.Milliseconds())
		if rw.status >= 500 {
			log.Printf("http level=error %s\n", meta)
		} else if rw.status >= 400 {
			log.Printf("http level=warn %s\n", meta)
		} else {
			log.Printf("http level=info %s\n", meta)
		}
	})
}
