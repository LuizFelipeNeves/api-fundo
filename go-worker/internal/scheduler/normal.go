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
			collector:      "market_snapshot",
			refillInterval: time.Minute,
			enabled:        s.shouldRunMarketSnapshot,
		},
		{
			collector:      "dividend_yield_chart",
			refillInterval: s.cfg.SchedulerInterval,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsWithZeroYield(ctx, s.cfg.BatchSize)
			},
		},
		{
			collector:      "fund_pipeline",
			refillInterval: s.cfg.SchedulerInterval,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				detailsIntervalMin := s.cfg.IntervalFundDetailsMin
				cotationsIntervalMin := s.cfg.IntervalCotationsMin
				nowLocal := time.Now().In(s.location)
				if os.Getenv("FORCE_RUN_JOBS") != "true" && !s.isBusinessHours(nowLocal) {
					detailsIntervalMin = 1_000_000
					cotationsIntervalMin = 1_000_000
				}

				var cutoff *time.Time
				if os.Getenv("FORCE_RUN_JOBS") == "true" {
					t := time.Now().Add(-time.Minute * time.Duration(s.cfg.IntervalIndicatorsMin))
					cutoff = &t
				} else {
					if s.isIndicatorsWindow(nowLocal) {
						t, ok := s.indicatorWindowStart(nowLocal)
						if ok {
							cutoff = &t
						}
					}
				}

				return s.db.SelectFundsForPipeline(
					ctx,
					detailsIntervalMin,
					s.cfg.IntervalDocumentsMin,
					cotationsIntervalMin,
					cutoff,
					s.cfg.BatchSize,
				)
			},
		},
	}

	if s.cfg.SchedulerInterval > 0 && len(iters) > 0 {
		spread := s.cfg.SchedulerInterval / time.Duration(len(iters))
		for i := range iters {
			if iters[i].collector == "market_snapshot" {
				continue
			}
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
