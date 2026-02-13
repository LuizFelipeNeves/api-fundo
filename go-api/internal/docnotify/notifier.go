package docnotify

import (
	"context"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/telegram"
)

type Notifier struct {
	DB        *db.DB
	Telegram  *telegram.Client
	FormatMsg func(fundCode string, d model.DocumentData) string
}

func (n *Notifier) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	if n.Telegram == nil || n.DB == nil || n.FormatMsg == nil {
		return
	}
	if n.Telegram.Token == "" {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cycleCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
				err := n.runCycle(cycleCtx)
				cancel()
				if err != nil {
					log.Printf("[doc_notify] error: %v\n", err)
				}
			}
		}
	}()
}

func (n *Notifier) runCycle(ctx context.Context) error {
	const lockKey int64 = 991337114
	var locked bool
	if err := n.DB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer func() {
		_, _ = n.DB.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", lockKey)
	}()

	type pendingRow struct {
		fundCode string
		doc      model.DocumentData
	}

	rows, err := n.DB.QueryContext(ctx, `
		SELECT fund_code, document_id, title, category, type, date, "dateUpload", url, status, version
		FROM document
		WHERE "send" = FALSE
		ORDER BY created_at ASC
		LIMIT 200
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	pending := make([]pendingRow, 0, 200)
	fundSet := map[string]struct{}{}
	for rows.Next() {
		var r pendingRow
		if err := rows.Scan(&r.fundCode, &r.doc.ID, &r.doc.Title, &r.doc.Category, &r.doc.Type, &r.doc.Date, &r.doc.DateUpload, &r.doc.URL, &r.doc.Status, &r.doc.Version); err != nil {
			return err
		}
		pending = append(pending, r)
		fundSet[r.fundCode] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	funds := make([]string, 0, len(fundSet))
	for k := range fundSet {
		funds = append(funds, k)
	}

	mapping := map[string][]string{}
	mapRows, err := n.DB.QueryContext(ctx, `
		SELECT fund_code, chat_id
		FROM telegram_user_fund
		WHERE fund_code = ANY($1)
	`, pq.Array(funds))
	if err != nil {
		return err
	}
	defer mapRows.Close()

	for mapRows.Next() {
		var fundCode string
		var chatID string
		if err := mapRows.Scan(&fundCode, &chatID); err != nil {
			return err
		}
		mapping[fundCode] = append(mapping[fundCode], chatID)
	}
	if err := mapRows.Err(); err != nil {
		return err
	}

	for _, it := range pending {
		chatIDs := mapping[it.fundCode]
		msg := n.FormatMsg(it.fundCode, it.doc)

		allSent := true
		for _, chatID := range chatIDs {
			if err := n.Telegram.SendMessage(ctx, chatID, msg); err != nil {
				allSent = false
				log.Printf("[doc_notify] send error fund=%s doc=%d chat_id=%s err=%v\n", it.fundCode, it.doc.ID, chatID, err)
			}
		}

		if len(chatIDs) == 0 {
			allSent = true
		}

		if allSent {
			if _, err := n.DB.ExecContext(ctx, `UPDATE document SET "send" = TRUE WHERE fund_code = $1 AND document_id = $2`, it.fundCode, it.doc.ID); err != nil {
				return err
			}
		}
	}

	return nil
}
