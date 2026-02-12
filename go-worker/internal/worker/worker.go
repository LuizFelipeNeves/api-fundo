package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/scheduler"
)

// Worker processes work items from the scheduler
type Worker struct {
	id        int
	registry  *collectors.Registry
	persister *persistence.Persister
	workChan  <-chan scheduler.WorkItem
}

// New creates a new worker
func New(id int, registry *collectors.Registry, persister *persistence.Persister, workChan <-chan scheduler.WorkItem) *Worker {
	return &Worker{
		id:        id,
		registry:  registry,
		persister: persister,
		workChan:  workChan,
	}
}

// Start begins processing work items
func (w *Worker) Start(ctx context.Context) error {
	log.Printf("[worker-%d] started\n", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] shutting down\n", w.id)
			return ctx.Err()
		case item := <-w.workChan:
			if err := w.processWorkItem(ctx, item); err != nil {
				log.Printf("[worker-%d] error processing %s for %s: %v\n",
					w.id, item.CollectorName, item.FundCode, err)
			}
		}
	}
}

// processWorkItem processes a single work item: collect → persist
func (w *Worker) processWorkItem(ctx context.Context, item scheduler.WorkItem) error {
	// Get the collector
	collector, err := w.registry.Get(item.CollectorName)
	if err != nil {
		return fmt.Errorf("collector not found: %w", err)
	}

	// Collect data
	req := collectors.CollectRequest{
		Collector:   item.CollectorName,
		FundCode:    item.FundCode,
		CNPJ:        item.CNPJ,
		ID:          item.ID,
		TriggeredBy: "scheduler",
	}

	result, err := collector.Collect(ctx, req)
	if err != nil {
		return fmt.Errorf("collection failed: %w", err)
	}

	// Persist data based on collector type
	if err := w.persistResult(ctx, item.CollectorName, result); err != nil {
		return fmt.Errorf("persistence failed: %w", err)
	}

	log.Printf("[worker-%d] completed %s for %s\n", w.id, item.CollectorName, item.FundCode)
	return nil
}

// persistResult persists the collection result
func (w *Worker) persistResult(ctx context.Context, collectorName string, result *collectors.CollectResult) error {
	switch collectorName {
	case "fund_list":
		items, ok := result.Data.([]collectors.FundListItem)
		if !ok {
			return fmt.Errorf("invalid data type for fund_list")
		}
		return w.persister.PersistFundList(ctx, items)

	case "fund_details":
		details, ok := result.Data.(collectors.FundDetails)
		if !ok {
			return fmt.Errorf("invalid data type for fund_details")
		}
		return w.persister.PersistFundDetails(ctx, details)

	case "indicators":
		indicators, ok := result.Data.(collectors.Indicators)
		if !ok {
			return fmt.Errorf("invalid data type for indicators")
		}
		return w.persister.PersistIndicators(ctx, result.FundCode, indicators)

	case "cotations_today":
		cotations, ok := result.Data.(collectors.CotationsResponse)
		if !ok {
			return fmt.Errorf("invalid data type for cotations_today")
		}
		return w.persister.PersistCotationsToday(ctx, result.FundCode, cotations)

	case "cotations":
		cotations, ok := result.Data.(collectors.CotationsResponse)
		if !ok {
			return fmt.Errorf("invalid data type for cotations")
		}
		return w.persister.PersistHistoricalCotations(ctx, result.FundCode, cotations)

	case "documents":
		documents, ok := result.Data.([]collectors.Document)
		if !ok {
			return fmt.Errorf("invalid data type for documents")
		}
		return w.persister.PersistDocuments(ctx, result.FundCode, documents)

	default:
		return fmt.Errorf("unknown collector: %s", collectorName)
	}
}
