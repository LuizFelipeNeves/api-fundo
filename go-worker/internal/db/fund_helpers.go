package db

import (
	"context"
	"database/sql"
	"fmt"
)

// GetFundIDByCode retrieves fund ID from database by code
func (db *DB) GetFundIDByCode(ctx context.Context, code string) (string, error) {
	query := `SELECT id FROM fund_master WHERE code = $1 LIMIT 1`

	var id string
	err := db.QueryRowContext(ctx, query, code).Scan(&id)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("fund not found: %s", code)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get fund ID: %w", err)
	}

	return id, nil
}

// GetFundCNPJByCode retrieves fund CNPJ from database by code
func (db *DB) GetFundCNPJByCode(ctx context.Context, code string) (string, error) {
	query := `SELECT cnpj FROM fund_master WHERE code = $1 LIMIT 1`

	var cnpj string
	err := db.QueryRowContext(ctx, query, code).Scan(&cnpj)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("fund not found: %s", code)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get fund CNPJ: %w", err)
	}

	return cnpj, nil
}

func (db *DB) GetLastDocumentsMaxIDByCode(ctx context.Context, code string) (int, error) {
	query := `SELECT last_documents_max_id FROM fund_state WHERE fund_code = $1 LIMIT 1`

	var maxID sql.NullInt64
	err := db.QueryRowContext(ctx, query, code).Scan(&maxID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get last documents max id: %w", err)
	}

	if !maxID.Valid || maxID.Int64 <= 0 {
		return 0, nil
	}

	return int(maxID.Int64), nil
}
