package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/scheduler"
)

var (
	inFlight       int64
	inFlightPeak   int64
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

func InFlightPeakAndReset() int64 {
	return atomic.SwapInt64(&inFlightPeak, 0)
}

func bumpPeak(dst *int64, v int64) {
	for {
		cur := atomic.LoadInt64(dst)
		if v <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(dst, cur, v) {
			return
		}
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
	mode      string

	// reuse struct to avoid allocation per job
	req collectors.CollectRequest

	consecutiveErrors int
}

// New creates a new worker
func New(
	id int,
	registry *collectors.Registry,
	persister *persistence.Persister,
	workChan <-chan scheduler.WorkItem,
	mode string,
) *Worker {
	return &Worker{
		id:        id,
		registry:  registry,
		persister: persister,
		workChan:  workChan,
		mode:      mode,
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
				w.consecutiveErrors++
				if err := sleepCtx(ctx, w.errorBackoff()); err != nil {
					return err
				}
				continue
			}

			w.consecutiveErrors = 0
		}
	}
}

// processWorkItem processes a single work item: collect → persist
func (w *Worker) processWorkItem(ctx context.Context, item scheduler.WorkItem) error {
	curInFlight := atomic.AddInt64(&inFlight, 1)
	bumpPeak(&inFlightPeak, curInFlight)
	defer atomic.AddInt64(&inFlight, -1)

	if item.CollectorName == "fund_pipeline" {
		if err := w.processFundPipeline(ctx, item); err != nil {
			return err
		}

		atomic.AddInt64(&processedTotal, 1)
		return nil
	}

	// registry lookup (map lookup is cheap)
	collector, err := w.registry.Get(item.CollectorName)
	if err != nil {
		return fmt.Errorf("collector not found: %w", err)
	}

	// reuse request struct (avoid allocation)
	w.req.FundCode = item.FundCode
	w.req.CNPJ = item.CNPJ
	w.req.ID = item.ID

	// Collect data
	result, err := collector.Collect(ctx, w.req)
	if err != nil {
		return fmt.Errorf("collection failed: %w", err)
	}

	// Persist data based on collector type
	if err := w.persistResult(ctx, item.CollectorName, item.FundCode, result); err != nil {
		return fmt.Errorf("persistence failed: %w", err)
	}

	drainLimit := 2
	if item.CollectorName == "market_snapshot" {
		drainLimit = 25
	}
	if w.mode == "backfill" {
		drainLimit = 50
	}
	if _, err := w.persister.DrainDirtyMetrics(ctx, drainLimit); err != nil {
		return fmt.Errorf("drain metrics failed: %w", err)
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

func (w *Worker) processFundPipeline(ctx context.Context, item scheduler.WorkItem) error {
	code := strings.TrimSpace(item.FundCode)
	if code == "" {
		return nil
	}

	mask := item.TaskMask
	if mask == 0 {
		return nil
	}

	run := func(collectorName string) error {
		collector, err := w.registry.Get(collectorName)
		if err != nil {
			return fmt.Errorf("collector not found: %w", err)
		}

		w.req.FundCode = code
		w.req.CNPJ = item.CNPJ
		w.req.ID = item.ID

		result, err := collector.Collect(ctx, w.req)
		if err != nil {
			return fmt.Errorf("collection failed: %w", err)
		}
		if err := w.persistResult(ctx, collectorName, code, result); err != nil {
			return fmt.Errorf("persistence failed: %w", err)
		}
		return nil
	}

	if mask&db.TaskDetails != 0 {
		if err := run("fund_details"); err != nil {
			return fmt.Errorf("pipeline fund_details failed: %w", err)
		}
	}
	if mask&db.TaskDocuments != 0 {
		if err := run("documents"); err != nil {
			return fmt.Errorf("pipeline documents failed: %w", err)
		}
	}
	if mask&db.TaskCotations != 0 {
		if err := run("cotations"); err != nil {
			return fmt.Errorf("pipeline cotations failed: %w", err)
		}
	}
	if mask&db.TaskIndicators != 0 {
		if err := run("indicators"); err != nil {
			return fmt.Errorf("pipeline indicators failed: %w", err)
		}
	}

	if err := w.persister.RecomputeMetricsForFund(ctx, code); err != nil {
		return fmt.Errorf("pipeline recompute metrics failed: %w", err)
	}

	return nil
}

func (w *Worker) errorBackoff() time.Duration {
	base := 500 * time.Millisecond
	if w.mode == "backfill" {
		base = 2 * time.Second
	}

	n := w.consecutiveErrors
	if n < 1 {
		return 0
	}
	if n > 6 {
		n = 6
	}

	d := base * time.Duration(1<<(n-1))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
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

	case "market_snapshot":
		data, ok := result.Data.(collectors.MarketSnapshotData)
		if !ok {
			return fmt.Errorf("invalid data type for market_snapshot")
		}
		return w.persister.PersistMarketSnapshot(ctx, data)

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

	case "dividend_yield_chart":
		data, ok := result.Data.(collectors.DividendYieldChartData)
		if !ok {
			return fmt.Errorf("invalid data type for dividend_yield_chart")
		}
		yields := collectors.ParseDividendYields(data.Items)
		return w.persister.PersistDividendYields(ctx, fundCode, yields)

	default:
		return fmt.Errorf("unknown collector: %s", collectorName)
	}
}
