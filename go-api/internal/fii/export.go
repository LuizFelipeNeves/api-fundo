package fii

import (
	"context"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func (s *Service) ExportFund(ctx context.Context, code string, opts ExportFundOptions) (*ExportFundJSON, bool, error) {
	details, err := s.GetFundDetails(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if details == nil {
		return nil, false, nil
	}

	cotDays := clampInt(opts.CotationsDays, 1825, 1, 5000)
	snapLimit := clampInt(opts.IndicatorsSnapshotsLimit, 365, 1, 5000)

	cotations, err := s.GetCotations(ctx, code, cotDays)
	if err != nil {
		return nil, false, err
	}

	dividends, _, err := s.GetDividends(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if dividends == nil {
		dividends = []model.DividendData{}
	}

	snapshots, err := s.GetLatestIndicatorsSnapshots(ctx, code, snapLimit)
	if err != nil {
		return nil, false, err
	}

	today, _, err := s.GetLatestCotationsToday(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if today == nil {
		today = []model.CotationTodayItem{}
	}

	out := buildExportFundJSON(details, cotations, dividends, snapshots, today, cotDays)
	return &out, true, nil
}
