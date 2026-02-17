package fii

import (
	"context"
	"database/sql"
	"time"
)

func (s *Service) GetLatestIndicatorsSnapshots(ctx context.Context, code string, limit int) ([]IndicatorsSnapshot, error) {
	_ = limit

	parsed, ok, err := s.GetLatestIndicators(ctx, code)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []IndicatorsSnapshot{}, nil
	}

	var maxFetched sql.NullTime
	err = s.DB.QueryRowContext(ctx, `
		SELECT MAX(fetched_at)
		FROM indicators_snapshot
		WHERE fund_code = $1
	`, code).Scan(&maxFetched)
	if err != nil {
		return nil, err
	}
	fetchedAt := time.Now().UTC()
	if maxFetched.Valid {
		fetchedAt = maxFetched.Time.UTC()
	}

	return []IndicatorsSnapshot{
		{
			FetchedAt: fetchedAt.Format("2006-01-02T15:04:05.000Z"),
			Data:      parsed,
		},
	}, nil
}
