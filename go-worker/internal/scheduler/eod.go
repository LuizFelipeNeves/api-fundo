package scheduler

import (
	"context"
	"database/sql"
	"fmt"
)

func runEODCotation(ctx context.Context, tx *sql.Tx, dateISO string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT fund_code, price
		FROM (
			SELECT DISTINCT ON (fund_code)
				fund_code,
				price
			FROM cotation_today
			WHERE date_iso = $1
				AND price > 0
				AND length(hour) = 5
				AND substring(hour, 3, 1) = ':'
			ORDER BY fund_code, hour DESC, fetched_at DESC
		) t
	`, dateISO)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation (fund_code, date_iso, price)
		VALUES ($1, $2, $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			price = EXCLUDED.price
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	inserted := 0
	for rows.Next() {
		var fundCode string
		var price float64
		if err := rows.Scan(&fundCode, &price); err != nil {
			return inserted, err
		}

		if price <= 0 {
			continue
		}

		if _, err := stmt.ExecContext(ctx, fundCode, dateISO, price); err != nil {
			return inserted, fmt.Errorf("insert cotation fund=%s date=%s: %w", fundCode, dateISO, err)
		}
		inserted++
	}
	if err := rows.Err(); err != nil {
		return inserted, err
	}

	return inserted, nil
}
