package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/scheduler"
)

var (
	inFlight       int64
	processedTotal int64
	errorTotal     int64
)

type StatsSnapshot struct {
	InFlight  int64
	Processed int64
	Errors    int64
}

func Stats() StatsSnapshot {
	return StatsSnapshot{
		InFlight:  atomic.LoadInt64(&inFlight),
		Processed: atomic.LoadInt64(&processedTotal),
		Errors:    atomic.LoadInt64(&errorTotal),
	}
}

func verboseLogs() bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("LOG_VERBOSE")), "true") {
		return true
	}
	level := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	return level == "debug" || level == "trace"
}

// Worker processes work items from the scheduler
type Worker struct {
	id        int
	registry  *collectors.Registry
	persister *persistence.Persister
	workChan  <-chan scheduler.WorkItem

	// reuse struct to avoid allocation per job
	req collectors.CollectRequest
}

// New creates a new worker
func New(
	id int,
	registry *collectors.Registry,
	persister *persistence.Persister,
	workChan <-chan scheduler.WorkItem,
) *Worker {
	return &Worker{
		id:        id,
		registry:  registry,
		persister: persister,
		workChan:  workChan,
	}
}

// Start begins processing work items
func (w *Worker) Start(ctx context.Context) error {
	if verboseLogs() {
		log.Println("[worker-", w.id, "] started")
	}

	for {
		select {

		case <-ctx.Done():
			log.Println("[worker-", w.id, "] shutting down")
			return ctx.Err()

		// SAFE channel read (avoid infinite loop if closed)
		case item, ok := <-w.workChan:
			if !ok {
				log.Println("[worker-", w.id, "] work channel closed")
				return nil
			}

			if err := w.processWorkItem(ctx, item); err != nil {
				atomic.AddInt64(&errorTotal, 1)
				log.Println(
					"[worker-", w.id,
					"] error processing ", item.CollectorName,
					" for ", item.FundCode,
					": ", err,
				)
			}
		}
	}
}

// processWorkItem processes a single work item: collect → persist
func (w *Worker) processWorkItem(ctx context.Context, item scheduler.WorkItem) error {
	atomic.AddInt64(&inFlight, 1)
	defer atomic.AddInt64(&inFlight, -1)

	// registry lookup (map lookup is cheap)
	collector, err := w.registry.Get(item.CollectorName)
	if err != nil {
		return fmt.Errorf("collector not found: %w", err)
	}

	// reuse request struct (avoid allocation)
	w.req.FundCode = item.FundCode
	w.req.CNPJ = item.CNPJ

	// Collect data
	result, err := collector.Collect(ctx, w.req)
	if err != nil {
		return fmt.Errorf("collection failed: %w", err)
	}

	// Persist data based on collector type
	if err := w.persistResult(ctx, item.CollectorName, item.FundCode, result); err != nil {
		return fmt.Errorf("persistence failed: %w", err)
	}

	atomic.AddInt64(&processedTotal, 1)

	if verboseLogs() {
		log.Println(
			"[worker-", w.id,
			"] completed ", item.CollectorName,
			" for ", item.FundCode,
		)
	}

	return nil
}

// persistResult persists the collection result
func (w *Worker) persistResult(
	ctx context.Context,
	collectorName string,
	fundCode string,
	result *collectors.CollectResult,
) error {

	switch collectorName {

	case "fund_list":
		items, ok := result.Data.([]collectors.FundListItem)
		if !ok {
			return fmt.Errorf("invalid data type for fund_list")
		}
		return w.persister.PersistFundList(ctx, items)

	case "fund_details":
		data, ok := result.Data.(collectors.FundDetailsData)
		if !ok {
			return fmt.Errorf("invalid data type for fund_details")
		}
		return w.persister.PersistFundDetails(ctx, fundCode, data)

	case "indicators":
		data, ok := result.Data.(collectors.IndicatorsData)
		if !ok {
			return fmt.Errorf("invalid data type for indicators")
		}
		return w.persister.PersistIndicators(ctx, data)

	case "cotations_today":
		data, ok := result.Data.(collectors.CotationsTodayData)
		if !ok {
			return fmt.Errorf("invalid data type for cotations_today")
		}
		return w.persister.PersistCotationsToday(ctx, data)

	case "cotations":
		items, ok := result.Data.([]collectors.CotationItem)
		if !ok {
			return fmt.Errorf("invalid data type for cotations")
		}
		return w.persister.PersistHistoricalCotations(ctx, fundCode, items)

	case "documents":
		items, ok := result.Data.([]collectors.DocumentItem)
		if !ok {
			return fmt.Errorf("invalid data type for documents")
		}
		return w.persister.PersistDocuments(ctx, fundCode, items)

	default:
		return fmt.Errorf("unknown collector: %s", collectorName)
	}
}
