package scheduler

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
)

func (s *Scheduler) startNormal(ctx context.Context) error {
	iters := []iteratorState{
		{
			collector:      "fund_list",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isIndicatorsWindow,
		},
		{
			collector:      "fund_details",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isBusinessHours,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForDetails(ctx, s.cfg.IntervalFundDetailsMin, s.cfg.BatchSize)
			},
		},
		{
			collector:      "cotations_today",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isBusinessHours,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForCotationsToday(ctx, s.cfg.IntervalCotationsTodayMin, s.cfg.BatchSize)
			},
		},
		{
			collector:      "documents",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isBusinessHours,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForDocuments(ctx, s.cfg.IntervalDocumentsMin, s.cfg.BatchSize)
			},
		},
		{
			collector:      "indicators",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isIndicatorsWindow,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				if os.Getenv("FORCE_RUN_JOBS") == "true" {
					return s.db.SelectFundsForIndicators(ctx, s.cfg.IntervalIndicatorsMin, s.cfg.BatchSize)
				}

				now := time.Now().In(s.location)
				cutoff, ok := s.indicatorWindowStart(now)
				if !ok {
					return nil, nil
				}

				return s.db.SelectFundsForIndicatorsWindow(ctx, cutoff, s.cfg.BatchSize)
			},
		},
	}

	if s.cfg.SchedulerInterval > 0 && len(iters) > 0 {
		spread := s.cfg.SchedulerInterval / time.Duration(len(iters))
		for i := range iters {
			iters[i].phase = time.Duration(i) * spread
		}
	}

	rrIndex := 0
	lastEODDate := ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		now := time.Now().In(s.location)

		if s.shouldRunEOD(now) {
			dateISO := now.Format("2006-01-02")
			if dateISO != lastEODDate {
				s.scheduleEODCotation(ctx, dateISO)
				lastEODDate = dateISO
			}
		}

		if cap(s.workChan) > 0 && len(s.workChan) >= cap(s.workChan) {
			if err := sleepCtx(ctx, 50*time.Millisecond); err != nil {
				return err
			}
			continue
		}

		dispatched := false
		for step := 0; step < len(iters); step++ {
			i := rrIndex + step
			if i >= len(iters) {
				i -= len(iters)
			}

			item, ok := iters[i].peek(ctx, now)
			if !ok {
				continue
			}

			select {
			case s.workChan <- item:
				iters[i].commit()
				atomic.AddInt64(&enqueuedTotal, 1)
				bumpPeakInt(&queueLenPeak, len(s.workChan))
				rrIndex = i + 1
				if rrIndex >= len(iters) {
					rrIndex = 0
				}

				dispatched = true

			case <-ctx.Done():
				return ctx.Err()

			default:
				rrIndex = i + 1
				if rrIndex >= len(iters) {
					rrIndex = 0
				}

				if err := sleepCtx(ctx, 50*time.Millisecond); err != nil {
					return err
				}
			}

			break
		}

		if dispatched {
			activeTypes := countActiveTypes(iters)
			gap := dispatchGap(activeTypes, len(s.workChan), cap(s.workChan), s.cfg.WorkerPoolSize)
			if err := sleepCtx(ctx, gap); err != nil {
				return err
			}
			continue
		}

		wake := nextWake(now, iters)
		sleepFor := time.Until(wake)
		if sleepFor < 25*time.Millisecond {
			sleepFor = 25 * time.Millisecond
		}
		if sleepFor > 500*time.Millisecond {
			sleepFor = 500 * time.Millisecond
		}

		if err := sleepCtx(ctx, sleepFor); err != nil {
			return err
		}
	}
}

func (s *Scheduler) isIndicatorsWindow(now time.Time) bool {
	if os.Getenv("FORCE_RUN_JOBS") == "true" {
		return true
	}

	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	total := now.Hour()*60 + now.Minute()
	return (total >= 540 && total <= 550) || (total >= 1140 && total <= 1150)
}

func (s *Scheduler) indicatorWindowStart(now time.Time) (time.Time, bool) {
	total := now.Hour()*60 + now.Minute()
	y, m, d := now.Date()

	if total >= 540 && total <= 550 {
		return time.Date(y, m, d, 9, 0, 0, 0, s.location), true
	}
	if total >= 1140 && total <= 1150 {
		return time.Date(y, m, d, 19, 0, 0, 0, s.location), true
	}

	return time.Time{}, false
}
