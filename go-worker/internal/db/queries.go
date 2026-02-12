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
	Code string
	CNPJ string
	ID   string
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
