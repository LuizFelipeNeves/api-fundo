package scheduler

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
)

// WorkItem represents a unit of work to be processed
type WorkItem struct {
	CollectorName string
	FundCode      string
	CNPJ          string
	ID            string
}

type iteratorState struct {
	collector        string
	refillInterval   time.Duration
	phase            time.Duration
	nextRefill       time.Time
	enabled          func(now time.Time) bool
	refill           func(ctx context.Context) ([]db.FundCandidate, error)
	candidates       []db.FundCandidate
	candidateIndex   int
	singletonPending bool
}

func nextRefillWithPhase(now time.Time, interval, phase time.Duration) time.Time {
	if interval <= 0 {
		return now
	}

	if phase < 0 {
		phase = 0
	}
	if phase >= interval {
		phase = phase % interval
	}

	shifted := now.Add(-phase)
	next := shifted.Truncate(interval).Add(interval).Add(phase)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

func (it *iteratorState) hasBuffered() bool {
	if it.collector == "fund_list" {
		return it.singletonPending
	}
	return it.candidateIndex < len(it.candidates)
}

func (it *iteratorState) peek(ctx context.Context, now time.Time) (WorkItem, bool) {

	if it.enabled != nil && !it.enabled(now) {
		it.candidates = nil
		it.candidateIndex = 0
		it.singletonPending = false
		return WorkItem{}, false
	}

	// ================= FUND LIST =================

	if it.collector == "fund_list" {

		if !it.singletonPending {

			if it.nextRefill.IsZero() || !now.Before(it.nextRefill) {

				it.singletonPending = true
				it.nextRefill = nextRefillWithPhase(now, it.refillInterval, it.phase)

			} else {
				return WorkItem{}, false
			}
		}

		return WorkItem{CollectorName: it.collector}, true
	}

	// ================= BUFFER =================

	if it.candidateIndex < len(it.candidates) {

		c := it.candidates[it.candidateIndex]

		return WorkItem{
			CollectorName: it.collector,
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}

	// ================= REFILL =================

	if !it.nextRefill.IsZero() && now.Before(it.nextRefill) {
		return WorkItem{}, false
	}

	if it.refill == nil {
		return WorkItem{}, false
	}

	candidates, err := it.refill(ctx)

	if err != nil {

		log.Println("[scheduler]", it.collector, "refill error:", err)

		// 🔥 retry rápido (evita freeze)
		it.nextRefill = now.Add(5 * time.Second)

		return WorkItem{}, false
	}

	// sucesso

	it.nextRefill = nextRefillWithPhase(now, it.refillInterval, it.phase)
	it.candidateIndex = 0
	it.candidates = candidates

	if len(it.candidates) == 0 {
		return WorkItem{}, false
	}

	c := it.candidates[it.candidateIndex]

	return WorkItem{
		CollectorName: it.collector,
		FundCode:      c.Code,
		CNPJ:          c.CNPJ,
		ID:            c.ID,
	}, true
}

func (it *iteratorState) commit() {
	if it.collector == "fund_list" {
		it.singletonPending = false
		return
	}

	if it.candidateIndex < len(it.candidates) {
		it.candidateIndex++
		if it.candidateIndex >= len(it.candidates) {
			it.candidates = nil
			it.candidateIndex = 0
		}
	}
}

func dispatchGap(activeTypes, queued, capacity, workerPoolSize int) time.Duration {
	if capacity <= 0 {
		return 50 * time.Millisecond
	}

	if workerPoolSize <= 0 {
		workerPoolSize = 1
	}

	lowWater := workerPoolSize
	highWater := workerPoolSize * 2
	if highWater > capacity-1 {
		highWater = capacity - 1
	}
	if highWater < lowWater {
		highWater = lowWater
	}

	gap := 40 * time.Millisecond
	if queued < lowWater {
		gap = 20 * time.Millisecond
	}
	if queued >= highWater {
		gap = 120 * time.Millisecond
	}
	if queued >= capacity-1 {
		gap = 250 * time.Millisecond
	}

	if activeTypes <= 1 {
		gap *= 3
	}

	return gap
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextWake(now time.Time, iters []iteratorState) time.Time {
	next := time.Time{}
	for i := range iters {
		it := &iters[i]
		if it.hasBuffered() {
			return now
		}
		if it.nextRefill.IsZero() {
			return now
		}
		if next.IsZero() || it.nextRefill.Before(next) {
			next = it.nextRefill
		}
	}
	if next.IsZero() {
		return now.Add(250 * time.Millisecond)
	}
	return next
}

func countActiveTypes(iters []iteratorState) int {

	active := 0

	now := time.Now()

	for i := range iters {

		if iters[i].enabled != nil && !iters[i].enabled(now) {
			continue
		}

		active++
	}

	if active == 0 {
		return 1
	}

	return active
}

// Scheduler manages scheduling
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
				s.scheduleEODCotation(ctx)
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

// ================= EOD + BUSINESS HOURS =================

func (s *Scheduler) scheduleEODCotation(ctx context.Context) {

	lockKey := int64(4419270101)

	err := s.db.TryAdvisoryLock(ctx, lockKey, func(tx *sql.Tx) error {
		log.Println("[scheduler] processing EOD cotation")
		return nil
	})

	if err != nil {
		log.Println("[scheduler] EOD error:", err)
	}
}

func (s *Scheduler) isBusinessHours(now time.Time) bool {

	if os.Getenv("FORCE_RUN_JOBS") == "true" {
		return true
	}

	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	total := now.Hour()*60 + now.Minute()

	return total >= 600 && total <= 1110
}

func (s *Scheduler) shouldRunEOD(now time.Time) bool {

	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	total := now.Hour()*60 + now.Minute()

	return total > 1110
}
