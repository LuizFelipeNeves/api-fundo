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

type workIterator func(ctx context.Context) (WorkItem, bool)

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

	ticker := time.NewTicker(s.cfg.SchedulerInterval)
	defer ticker.Stop()

	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {

	now := time.Now().In(s.location)
	shouldRunBusinessHours := s.isBusinessHours(now)

	var iters []workIterator

	if shouldRunBusinessHours {

		iters = append(iters,
			s.iterFundList(ctx),
			s.iterFundDetails(ctx),
			s.iterCotationsToday(ctx),
			s.iterIndicators(ctx),
		)
	}

	iters = append(iters,
		s.iterHistoricalCotations(ctx),
		s.iterDocuments(ctx),
	)

	s.dispatchFair(ctx, iters)

	if s.shouldRunEOD(now) {
		s.scheduleEODCotation(ctx)
	}
}

func (s *Scheduler) dispatchFair(ctx context.Context, iters []workIterator) {

	active := len(iters)

	for active > 0 {

		for i := range iters {

			iter := iters[i]
			if iter == nil {
				continue
			}

			item, ok := iter(ctx)
			if !ok {
				iters[i] = nil
				active--
				continue
			}

			select {

			case s.workChan <- item:
				// scheduled

			case <-ctx.Done():
				return

			default:
				// channel cheio → parar para não alocar buffer
				return
			}
		}
	}
}

// ================= ITERATORS =================

func (s *Scheduler) iterFundList(ctx context.Context) workIterator {

	scheduled := false

	return func(ctx context.Context) (WorkItem, bool) {

		if scheduled {
			return WorkItem{}, false
		}

		scheduled = true

		return WorkItem{
			CollectorName: "fund_list",
		}, true
	}
}

func (s *Scheduler) iterFundDetails(ctx context.Context) workIterator {

	candidates, err := s.db.SelectFundsForDetails(ctx, s.cfg.IntervalFundDetailsMin, s.cfg.BatchSize)
	if err != nil {
		log.Println("[scheduler] fund_details error:", err)
		return nil
	}

	index := 0

	return func(ctx context.Context) (WorkItem, bool) {

		if index >= len(candidates) {
			return WorkItem{}, false
		}

		c := candidates[index]
		index++

		return WorkItem{
			CollectorName: "fund_details",
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}
}

func (s *Scheduler) iterCotationsToday(ctx context.Context) workIterator {

	candidates, err := s.db.SelectFundsForCotationsToday(ctx, s.cfg.IntervalCotationsTodayMin, s.cfg.BatchSize)
	if err != nil {
		log.Println("[scheduler] cotations_today error:", err)
		return nil
	}

	index := 0

	return func(ctx context.Context) (WorkItem, bool) {

		if index >= len(candidates) {
			return WorkItem{}, false
		}

		c := candidates[index]
		index++

		return WorkItem{
			CollectorName: "cotations_today",
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}
}

func (s *Scheduler) iterIndicators(ctx context.Context) workIterator {

	candidates, err := s.db.SelectFundsForIndicators(ctx, s.cfg.IntervalIndicatorsMin, s.cfg.BatchSize)
	if err != nil {
		log.Println("[scheduler] indicators error:", err)
		return nil
	}

	index := 0

	return func(ctx context.Context) (WorkItem, bool) {

		if index >= len(candidates) {
			return WorkItem{}, false
		}

		c := candidates[index]
		index++

		return WorkItem{
			CollectorName: "indicators",
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}
}

func (s *Scheduler) iterHistoricalCotations(ctx context.Context) workIterator {

	backfillInterval := 100 * 365 * 24 * 60

	candidates, err := s.db.SelectFundsForHistoricalCotations(ctx, backfillInterval, s.cfg.BatchSize)
	if err != nil {
		log.Println("[scheduler] cotations error:", err)
		return nil
	}

	index := 0

	return func(ctx context.Context) (WorkItem, bool) {

		if index >= len(candidates) {
			return WorkItem{}, false
		}

		c := candidates[index]
		index++

		return WorkItem{
			CollectorName: "cotations",
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}
}

func (s *Scheduler) iterDocuments(ctx context.Context) workIterator {

	candidates, err := s.db.SelectFundsForDocuments(ctx, s.cfg.IntervalDocumentsMin, s.cfg.BatchSize)
	if err != nil {
		log.Println("[scheduler] documents error:", err)
		return nil
	}

	index := 0

	return func(ctx context.Context) (WorkItem, bool) {

		if index >= len(candidates) {
			return WorkItem{}, false
		}

		c := candidates[index]
		index++

		return WorkItem{
			CollectorName: "documents",
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
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
