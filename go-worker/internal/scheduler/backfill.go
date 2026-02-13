package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
)

func (s *Scheduler) startBackfill(ctx context.Context) error {
	if err := s.backfillFundList(ctx); err != nil {
		return err
	}

	if err := s.runBackfillStage(
		ctx,
		[]iteratorState{
			{
				collector:      "fund_details",
				refillInterval: s.cfg.SchedulerInterval,
				refill: func(ctx context.Context) ([]db.FundCandidate, error) {
					return s.db.SelectFundsMissingDetails(ctx, s.cfg.BatchSize)
				},
			},
			{
				collector:      "cotations_today",
				refillInterval: s.cfg.SchedulerInterval,
				refill: func(ctx context.Context) ([]db.FundCandidate, error) {
					return s.db.SelectFundsMissingCotationsToday(ctx, s.cfg.BatchSize)
				},
			},
		},
		func(ctx context.Context) (int, error) {
			a, err := s.db.CountFundsMissingDetails(ctx)
			if err != nil {
				return 0, err
			}
			b, err := s.db.CountFundsMissingCotationsToday(ctx)
			if err != nil {
				return 0, err
			}
			return a + b, nil
		},
	); err != nil {
		return err
	}

	if err := s.runBackfillStage(
		ctx,
		[]iteratorState{
			{
				collector:      "documents",
				refillInterval: s.cfg.SchedulerInterval,
				refill: func(ctx context.Context) ([]db.FundCandidate, error) {
					return s.db.SelectFundsMissingDocuments(ctx, s.cfg.BatchSize)
				},
			},
			{
				collector:      "cotations",
				refillInterval: s.cfg.SchedulerInterval,
				refill: func(ctx context.Context) ([]db.FundCandidate, error) {
					return s.db.SelectFundsMissingHistoricalCotations(ctx, s.cfg.BatchSize)
				},
			},
		},
		func(ctx context.Context) (int, error) {
			a, err := s.db.CountFundsMissingDocuments(ctx)
			if err != nil {
				return 0, err
			}
			b, err := s.db.CountFundsMissingHistoricalCotations(ctx)
			if err != nil {
				return 0, err
			}
			return a + b, nil
		},
	); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		updated, err := s.persister.RecomputeDividendYields(ctx)
		if err == nil {
			log.Printf("[backfill] dividend yields updated=%d\n", updated)
			break
		}

		log.Println("[backfill] dividend yield recompute error:", err)
		if err := sleepCtx(ctx, 5*time.Second); err != nil {
			return err
		}
	}

	if err := s.runBackfillStage(
		ctx,
		[]iteratorState{
			{
				collector:      "indicators",
				refillInterval: s.cfg.SchedulerInterval,
				refill: func(ctx context.Context) ([]db.FundCandidate, error) {
					return s.db.SelectFundsMissingIndicators(ctx, s.cfg.BatchSize)
				},
			},
		},
		func(ctx context.Context) (int, error) {
			return s.db.CountFundsMissingIndicators(ctx)
		},
	); err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) backfillFundList(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		collector, err := s.registry.Get("fund_list")
		if err != nil {
			return fmt.Errorf("collector not found: %w", err)
		}

		res, err := collector.Collect(ctx, collectors.CollectRequest{})
		if err == nil {
			items, ok := res.Data.([]collectors.FundListItem)
			if !ok {
				return fmt.Errorf("invalid data type for fund_list")
			}
			if err := s.persister.PersistFundList(ctx, items); err == nil {
				return nil
			}
		}

		log.Println("[backfill] fund_list error:", err)
		if err := sleepCtx(ctx, 5*time.Second); err != nil {
			return err
		}
	}
}

func (s *Scheduler) runBackfillStage(
	ctx context.Context,
	iters []iteratorState,
	remaining func(context.Context) (int, error),
) error {
	if s.cfg.SchedulerInterval > 0 && len(iters) > 0 {
		spread := s.cfg.SchedulerInterval / time.Duration(len(iters))
		for i := range iters {
			iters[i].phase = time.Duration(i) * spread
		}
	}

	rrIndex := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		now := time.Now().In(s.location)

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

		if len(s.workChan) == 0 {
			n, err := remaining(ctx)
			if err != nil {
				log.Println("[backfill] remaining check error:", err)
				if err := sleepCtx(ctx, 5*time.Second); err != nil {
					return err
				}
				continue
			}
			if n == 0 {
				return nil
			}
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
