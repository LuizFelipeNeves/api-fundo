package scheduler

import (
	"context"
	"database/sql"
	"fmt"
)

func runEODCotation(ctx context.Context, tx *sql.Tx, dateISO string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT fund_code, price_int
		FROM (
			SELECT DISTINCT ON (fund_code)
				fund_code,
				price_int
			FROM cotation_today
			WHERE date_iso = $1
				AND price_int > 0
			ORDER BY fund_code, hour DESC, fetched_at DESC
		) t
	`, dateISO)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation (fund_code, date_iso, price_int)
		VALUES ($1, $2, $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			price_int = EXCLUDED.price_int
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	stmtDirty, err := tx.PrepareContext(ctx, `
		INSERT INTO fund_state (fund_code, last_metrics_at, created_at, updated_at)
		VALUES ($1, NULL, NOW(), NOW())
		ON CONFLICT (fund_code) DO UPDATE SET
			last_metrics_at = NULL,
			updated_at = NOW()
	`)
	if err != nil {
		return 0, err
	}
	defer stmtDirty.Close()

	inserted := 0
	for rows.Next() {
		var fundCode string
		var priceInt int
		if err := rows.Scan(&fundCode, &priceInt); err != nil {
			return inserted, err
		}

		if priceInt <= 0 {
			continue
		}

		if _, err := stmt.ExecContext(ctx, fundCode, dateISO, priceInt); err != nil {
			return inserted, fmt.Errorf("insert cotation fund=%s date=%s: %w", fundCode, dateISO, err)
		}
		if _, err := stmtDirty.ExecContext(ctx, fundCode); err != nil {
			return inserted, fmt.Errorf("mark dirty fund=%s: %w", fundCode, err)
		}
		inserted++
	}
	if err := rows.Err(); err != nil {
		return inserted, err
	}

	return inserted, nil
}
