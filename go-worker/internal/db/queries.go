package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// FundCandidate represents a fund selected for processing
type FundCandidate struct {
	Code     string
	CNPJ     string
	ID       string
	TaskMask int
}

const (
	TaskDetails    = 1
	TaskDocuments  = 2
	TaskIndicators = 4
	TaskCotations  = 8
	TaskYield      = 16
)

func (db *DB) SelectFundsForPipeline(ctx context.Context, detailsIntervalMin, documentsIntervalMin, cotationsIntervalMin int, indicatorsCutoff *time.Time, limit int) ([]FundCandidate, error) {
	if limit <= 0 {
		limit = 1
	}

	var cutoff any = nil
	if indicatorsCutoff != nil && !indicatorsCutoff.IsZero() {
		cutoff = *indicatorsCutoff
	}

	rows, err := db.QueryContext(ctx, `
		WITH base AS (
			SELECT
				fm.code AS code,
				COALESCE(fm.cnpj, '') AS cnpj,
				COALESCE(fm.id, '') AS id,
				COALESCE(fs.last_details_sync_at, fm.created_at, '1970-01-01'::timestamptz) AS last_details,
				COALESCE(fs.last_documents_at, fm.created_at, '1970-01-01'::timestamptz) AS last_documents,
				COALESCE(fs.last_historical_cotations_at, fm.created_at, '1970-01-01'::timestamptz) AS last_cotations,
				COALESCE(fs.last_indicators_at, '1970-01-01'::timestamptz) AS last_indicators
			FROM fund_master fm
			LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		),
		scored AS (
			SELECT
				code,
				cnpj,
				id,
				(CASE WHEN last_details < NOW() - INTERVAL '1 minute' * $1 THEN $6 ELSE 0 END) +
				(CASE WHEN cnpj != '' AND last_documents < NOW() - INTERVAL '1 minute' * $2 THEN $7 ELSE 0 END) +
				(CASE WHEN id != '' AND last_cotations < NOW() - INTERVAL '1 minute' * $3 THEN $9 ELSE 0 END) +
				(CASE WHEN $4::timestamptz IS NOT NULL AND id != '' AND last_indicators < $4 THEN $8 ELSE 0 END)
				AS task_mask,
				LEAST(last_details, last_documents, last_cotations, last_indicators) AS sort_key
			FROM base
		)
		SELECT code, cnpj, id, task_mask
		FROM scored
		WHERE task_mask > 0
		ORDER BY sort_key ASC, code ASC
		LIMIT $5
	`, detailsIntervalMin, documentsIntervalMin, cotationsIntervalMin, cutoff, limit, TaskDetails, TaskDocuments, TaskIndicators, TaskCotations)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FundCandidate, 0, limit)
	for rows.Next() {
		var c FundCandidate
		if err := rows.Scan(&c.Code, &c.CNPJ, &c.ID, &c.TaskMask); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// SelectFundsForDetails selects funds that need details update based on last_details_sync_at
func (db *DB) SelectFundsForDetails(ctx context.Context, intervalMinutes, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE COALESCE(fs.last_details_sync_at, fm.created_at, '1970-01-01'::timestamptz) 
			< NOW() - INTERVAL '1 minute' * $1
		ORDER BY COALESCE(fs.last_details_sync_at, fm.created_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, intervalMinutes, limit)
}

// SelectFundsForIndicators selects funds that need indicators update
func (db *DB) SelectFundsForIndicators(ctx context.Context, intervalMinutes, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND COALESCE(fs.last_indicators_at, fm.created_at, '1970-01-01'::timestamptz) 
			< NOW() - INTERVAL '1 minute' * $1
		ORDER BY COALESCE(fs.last_indicators_at, fm.created_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, intervalMinutes, limit)
}

func (db *DB) SelectFundsForIndicatorsWindow(ctx context.Context, cutoff time.Time, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND COALESCE(fs.last_indicators_at, '1970-01-01'::timestamptz) < $1
		ORDER BY COALESCE(fs.last_indicators_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, cutoff, limit)
}

// SelectFundsForCotationsToday selects funds for today's cotations
func (db *DB) SelectFundsForCotationsToday(ctx context.Context, intervalMinutes, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE COALESCE(fs.last_cotations_today_at, fm.created_at, '1970-01-01'::timestamptz) 
			< NOW() - INTERVAL '1 minute' * $1
		ORDER BY COALESCE(fs.last_cotations_today_at, fm.created_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, intervalMinutes, limit)
}

func (db *DB) SelectFundsMissingDetails(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fs.last_details_sync_at IS NULL
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

func (db *DB) SelectFundsMissingCotationsToday(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fs.last_cotations_today_at IS NULL
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

// SelectFundsForHistoricalCotations selects funds for historical cotations backfill
func (db *DB) SelectFundsForHistoricalCotations(ctx context.Context, intervalMinutes, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND COALESCE(fs.last_historical_cotations_at, fm.created_at, '1970-01-01'::timestamptz) 
			< NOW() - INTERVAL '1 minute' * $1
		ORDER BY COALESCE(fs.last_historical_cotations_at, fm.created_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, intervalMinutes, limit)
}

func (db *DB) SelectFundsMissingHistoricalCotations(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND fs.last_historical_cotations_at IS NULL
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

// SelectFundsForDocuments selects funds for documents sync
func (db *DB) SelectFundsForDocuments(ctx context.Context, intervalMinutes, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.cnpj IS NOT NULL AND fm.cnpj != ''
			AND COALESCE(fs.last_documents_at, fm.created_at, '1970-01-01'::timestamptz) 
			< NOW() - INTERVAL '1 minute' * $1
		ORDER BY COALESCE(fs.last_documents_at, fm.created_at, '1970-01-01'::timestamptz) ASC
		LIMIT $2
	`
	return db.queryFundCandidates(ctx, query, intervalMinutes, limit)
}

func (db *DB) SelectFundsMissingDocuments(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.cnpj IS NOT NULL AND fm.cnpj != ''
			AND fs.last_documents_at IS NULL
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

func (db *DB) SelectFundsMissingIndicators(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND fs.last_indicators_at IS NULL
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

// SelectFundsWithZeroYield selects funds that have dividends with yield = 0 and type = 1
func (db *DB) SelectFundsWithZeroYield(ctx context.Context, limit int) ([]FundCandidate, error) {
	query := `
		SELECT DISTINCT fm.code, COALESCE(fm.cnpj, '') as cnpj, COALESCE(fm.id, '') as id
		FROM fund_master fm
		INNER JOIN dividend d ON fm.code = d.fund_code
		WHERE d.type = 1
			AND d.yield = 0
			AND d.value > 0
		ORDER BY fm.code ASC
		LIMIT $1
	`
	return db.queryFundCandidates(ctx, query, limit)
}

// SelectFundsForYieldBackfill selects funds that need yield backfill
func (db *DB) SelectFundsForYieldBackfill(ctx context.Context, limit int) ([]FundCandidate, error) {
	return db.SelectFundsWithZeroYield(ctx, limit)
}

// CountFundsWithZeroYield counts funds that have dividends with yield = 0 and type = 1
func (db *DB) CountFundsWithZeroYield(ctx context.Context) (int, error) {
	query := `
		SELECT COUNT(DISTINCT fund_code)
		FROM dividend
		WHERE type = 1
			AND yield = 0
			AND value > 0
	`
	return db.queryCount(ctx, query)
}

func (db *DB) CountFundsMissingDetails(ctx context.Context) (int, error) {
	return db.queryCount(ctx, `
		SELECT COUNT(*)
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fs.last_details_sync_at IS NULL
	`)
}

func (db *DB) CountFundsMissingCotationsToday(ctx context.Context) (int, error) {
	return db.queryCount(ctx, `
		SELECT COUNT(*)
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fs.last_cotations_today_at IS NULL
	`)
}

func (db *DB) CountFundsMissingDocuments(ctx context.Context) (int, error) {
	return db.queryCount(ctx, `
		SELECT COUNT(*)
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.cnpj IS NOT NULL AND fm.cnpj != ''
			AND fs.last_documents_at IS NULL
	`)
}

func (db *DB) CountFundsMissingHistoricalCotations(ctx context.Context) (int, error) {
	return db.queryCount(ctx, `
		SELECT COUNT(*)
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND fs.last_historical_cotations_at IS NULL
	`)
}

func (db *DB) CountFundsMissingIndicators(ctx context.Context) (int, error) {
	return db.queryCount(ctx, `
		SELECT COUNT(*)
		FROM fund_master fm
		LEFT JOIN fund_state fs ON fm.code = fs.fund_code
		WHERE fm.id IS NOT NULL AND fm.id != ''
			AND fs.last_indicators_at IS NULL
	`)
}

func (db *DB) queryCount(ctx context.Context, query string, args ...interface{}) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, query, args...).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// queryFundCandidates executes a query and returns fund candidates
func (db *DB) queryFundCandidates(ctx context.Context, query string, args ...interface{}) ([]FundCandidate, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []FundCandidate
	for rows.Next() {
		var c FundCandidate
		if err := rows.Scan(&c.Code, &c.CNPJ, &c.ID); err != nil {
			return nil, err
		}
		candidates = append(candidates, c)
	}

	return candidates, rows.Err()
}

// UpdateFundStateTimestamp updates a specific timestamp in fund_state
func (db *DB) UpdateFundStateTimestamp(ctx context.Context, fundCode, field string, timestamp time.Time) error {
	// Update the specific timestamp field
	var query string
	switch field {
	case "last_details_sync_at":
		query = "UPDATE fund_state SET last_details_sync_at = $2, updated_at = NOW() WHERE fund_code = $1"
	case "last_indicators_at":
		query = "UPDATE fund_state SET last_indicators_at = $2, updated_at = NOW() WHERE fund_code = $1"
	case "last_cotations_today_at":
		query = "UPDATE fund_state SET last_cotations_today_at = $2, updated_at = NOW() WHERE fund_code = $1"
	case "last_historical_cotations_at":
		query = "UPDATE fund_state SET last_historical_cotations_at = $2, updated_at = NOW() WHERE fund_code = $1"
	case "last_documents_at":
		query = "UPDATE fund_state SET last_documents_at = $2, updated_at = NOW() WHERE fund_code = $1"
	default:
		return nil // Unknown field, skip silently
	}

	if strings.TrimSpace(fundCode) == "" {
		return fmt.Errorf("fundCode is required")
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO fund_state (fund_code, created_at, updated_at)
		VALUES ($1, NOW(), NOW())
		ON CONFLICT (fund_code) DO NOTHING
	`, fundCode)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, query, fundCode, timestamp)
	return err
}

// TryAdvisoryLock attempts to acquire an advisory lock for EOD cotation
func (db *DB) TryAdvisoryLock(ctx context.Context, lockKey int64, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Try to acquire advisory lock
	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockKey).Scan(&locked)
	if err != nil {
		return err
	}

	if !locked {
		return nil // Lock not acquired, another worker is processing
	}

	// Execute the function with the transaction
	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}
