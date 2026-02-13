package scheduler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
)

type Scheduler struct {
	cfg       *config.Config
	db        *db.DB
	registry  *collectors.Registry
	persister *persistence.Persister
	workChan  chan WorkItem
	location  *time.Location
}

func New(cfg *config.Config, database *db.DB, registry *collectors.Registry, persister *persistence.Persister, workChan chan WorkItem) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		db:        database,
		registry:  registry,
		persister: persister,
		workChan:  workChan,
		location:  cfg.Location,
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	backfillInterval := 100 * 365 * 24 * 60

	iters := []iteratorState{
		{
			collector:      "fund_list",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isBusinessHours,
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
			collector:      "indicators",
			refillInterval: s.cfg.SchedulerInterval,
			enabled:        s.isBusinessHours,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForIndicators(ctx, s.cfg.IntervalIndicatorsMin, s.cfg.BatchSize)
			},
		},
		{
			collector:      "cotations",
			refillInterval: s.cfg.SchedulerInterval,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForHistoricalCotations(ctx, backfillInterval, s.cfg.BatchSize)
			},
		},
		{
			collector:      "documents",
			refillInterval: s.cfg.SchedulerInterval,
			refill: func(ctx context.Context) ([]db.FundCandidate, error) {
				return s.db.SelectFundsForDocuments(ctx, s.cfg.IntervalDocumentsMin, s.cfg.BatchSize)
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
