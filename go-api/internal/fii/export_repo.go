package fii

import (
	"context"
	"encoding/json"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func (s *Service) GetLatestIndicatorsSnapshots(ctx context.Context, code string, limit int) ([]IndicatorsSnapshot, error) {
	safe := clampInt(limit, 365, 1, 5000)

	rows, err := s.DB.QueryContext(ctx, `
		SELECT fetched_at, data_json
		FROM indicators_snapshot
		WHERE fund_code = $1
		ORDER BY fetched_at DESC
		LIMIT $2
	`, code, safe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IndicatorsSnapshot, 0, safe)
	for rows.Next() {
		var (
			fetched time.Time
			raw     []byte
		)
		if err := rows.Scan(&fetched, &raw); err != nil {
			return nil, err
		}
		var parsed model.NormalizedIndicators
		if err := json.Unmarshal(raw, &parsed); err != nil {
			continue
		}
		out = append(out, IndicatorsSnapshot{
			FetchedAt: fetched.UTC().Format("2006-01-02T15:04:05.000Z"),
			Data:      parsed,
		})
	}
	return out, rows.Err()
}
