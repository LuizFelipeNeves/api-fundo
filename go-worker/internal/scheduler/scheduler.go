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

// Scheduler manages the scheduling of data collection tasks
type Scheduler struct {
	cfg       *config.Config
	db        *db.DB
	registry  *collectors.Registry
	persister *persistence.Persister
	workChan  chan WorkItem
	location  *time.Location
}

// New creates a new scheduler
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

// Start begins the scheduler loop
func (s *Scheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.SchedulerInterval)
	defer ticker.Stop()

	// Run immediately on start
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

// tick performs one scheduling cycle
func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now().In(s.location)

	// Check if we should run business-hours-only tasks
	shouldRunBusinessHours := s.isBusinessHours(now)

	// 1. Fund list (every 30 minutes during business hours)
	if shouldRunBusinessHours {
		s.scheduleFundList(ctx)
	}

	// 2. Fund details (every 15 minutes during business hours)
	if shouldRunBusinessHours {
		s.scheduleFundDetails(ctx)
	}

	// 3. Cotations today (every 5 minutes during business hours)
	if shouldRunBusinessHours {
		s.scheduleCotationsToday(ctx)
	}

	// 4. Indicators (every 30 minutes during business hours)
	if shouldRunBusinessHours {
		s.scheduleIndicators(ctx)
	}

	// 5. Historical cotations (backfill, runs anytime)
	s.scheduleHistoricalCotations(ctx)

	// 6. Documents (every 25 minutes, runs anytime)
	s.scheduleDocuments(ctx)

	// 7. EOD cotation (once per day after 18:30)
	if s.shouldRunEOD(now) {
		s.scheduleEODCotation(ctx)
	}
}

// scheduleFundList schedules fund list collection
func (s *Scheduler) scheduleFundList(ctx context.Context) {
	select {
	case s.workChan <- WorkItem{CollectorName: "fund_list"}:
		log.Println("[scheduler] scheduled fund_list")
	case <-ctx.Done():
	default:
		// Channel full, skip
	}
}

// scheduleFundDetails schedules fund details collection
func (s *Scheduler) scheduleFundDetails(ctx context.Context) {
	candidates, err := s.db.SelectFundsForDetails(ctx, s.cfg.IntervalFundDetailsMin, s.cfg.BatchSize)
	if err != nil {
		log.Printf("[scheduler] error selecting funds for details: %v\n", err)
		return
	}

	for _, candidate := range candidates {
		select {
		case s.workChan <- WorkItem{
			CollectorName: "fund_details",
			FundCode:      candidate.Code,
			CNPJ:          candidate.CNPJ,
			ID:            candidate.ID,
		}:
			log.Printf("[scheduler] scheduled fund_details: %s\n", candidate.Code)
		case <-ctx.Done():
			return
		default:
			// Channel full, skip remaining
			return
		}
	}
}

// scheduleCotationsToday schedules today's cotations collection
func (s *Scheduler) scheduleCotationsToday(ctx context.Context) {
	candidates, err := s.db.SelectFundsForCotationsToday(ctx, s.cfg.IntervalCotationsTodayMin, s.cfg.BatchSize)
	if err != nil {
		log.Printf("[scheduler] error selecting funds for cotations_today: %v\n", err)
		return
	}

	for _, candidate := range candidates {
		select {
		case s.workChan <- WorkItem{
			CollectorName: "cotations_today",
			FundCode:      candidate.Code,
			CNPJ:          candidate.CNPJ,
			ID:            candidate.ID,
		}:
			log.Printf("[scheduler] scheduled cotations_today: %s\n", candidate.Code)
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

// scheduleIndicators schedules indicators collection
func (s *Scheduler) scheduleIndicators(ctx context.Context) {
	candidates, err := s.db.SelectFundsForIndicators(ctx, s.cfg.IntervalIndicatorsMin, s.cfg.BatchSize)
	if err != nil {
		log.Printf("[scheduler] error selecting funds for indicators: %v\n", err)
		return
	}

	for _, candidate := range candidates {
		select {
		case s.workChan <- WorkItem{
			CollectorName: "indicators",
			FundCode:      candidate.Code,
			CNPJ:          candidate.CNPJ,
			ID:            candidate.ID,
		}:
			log.Printf("[scheduler] scheduled indicators: %s\n", candidate.Code)
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

// scheduleHistoricalCotations schedules historical cotations backfill
func (s *Scheduler) scheduleHistoricalCotations(ctx context.Context) {
	// Use a very long interval for backfill (100 years in minutes)
	backfillInterval := 100 * 365 * 24 * 60
	candidates, err := s.db.SelectFundsForHistoricalCotations(ctx, backfillInterval, s.cfg.BatchSize)
	if err != nil {
		log.Printf("[scheduler] error selecting funds for historical cotations: %v\n", err)
		return
	}

	for _, candidate := range candidates {
		select {
		case s.workChan <- WorkItem{
			CollectorName: "cotations",
			FundCode:      candidate.Code,
			CNPJ:          candidate.CNPJ,
			ID:            candidate.ID,
		}:
			log.Printf("[scheduler] scheduled cotations: %s\n", candidate.Code)
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

// scheduleDocuments schedules documents collection
func (s *Scheduler) scheduleDocuments(ctx context.Context) {
	candidates, err := s.db.SelectFundsForDocuments(ctx, s.cfg.IntervalDocumentsMin, s.cfg.BatchSize)
	if err != nil {
		log.Printf("[scheduler] error selecting funds for documents: %v\n", err)
		return
	}

	for _, candidate := range candidates {
		select {
		case s.workChan <- WorkItem{
			CollectorName: "documents",
			FundCode:      candidate.Code,
			CNPJ:          candidate.CNPJ,
			ID:            candidate.ID,
		}:
			log.Printf("[scheduler] scheduled documents: %s\n", candidate.Code)
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

// scheduleEODCotation schedules end-of-day cotation processing
func (s *Scheduler) scheduleEODCotation(ctx context.Context) {
	// Use advisory lock to ensure only one worker processes EOD
	lockKey := int64(4419270101)

	err := s.db.TryAdvisoryLock(ctx, lockKey, func(tx *sql.Tx) error {
		// Process EOD cotation logic here
		// This would involve finalizing today's cotations
		log.Println("[scheduler] processing EOD cotation")
		return nil
	})

	if err != nil {
		log.Printf("[scheduler] error processing EOD cotation: %v\n", err)
	}
}

// isBusinessHours checks if current time is within business hours (10:00-18:30)
func (s *Scheduler) isBusinessHours(now time.Time) bool {
	// Check if it's a weekday (Monday-Friday)
	forceRun := os.Getenv("FORCE_RUN_JOBS") == "true"

	if forceRun {
		return true
	}

	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	hour := now.Hour()
	minute := now.Minute()
	totalMinutes := hour*60 + minute

	// Business hours: 10:00 (600 minutes) to 18:30 (1110 minutes)
	return totalMinutes >= 600 && totalMinutes <= 1110
}

// shouldRunEOD checks if EOD cotation should run (after 18:30 on weekdays)
func (s *Scheduler) shouldRunEOD(now time.Time) bool {
	// Check if it's a weekday
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	hour := now.Hour()
	minute := now.Minute()
	totalMinutes := hour*60 + minute

	// After 18:30 (1110 minutes)
	return totalMinutes > 1110
}
