package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/scheduler"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/worker"
)

func statsInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv("WORKER_STATS_INTERVAL"))
	if raw == "" {
		return 30 * time.Second
	}
	if raw == "0" || raw == "0s" {
		return 0
	}

	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}

	if seconds, err := strconv.Atoi(raw); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}

	return 30 * time.Second
}

func applyDatabaseSchema(ctx context.Context, database *db.DB) error {
	cwd, _ := os.Getwd()

	candidates := []string{
		strings.TrimSpace(os.Getenv("DATABASE_SCHEMA_PATH")),
		filepath.Join(cwd, "database", "schema.sql"),
		filepath.Join(cwd, "..", "database", "schema.sql"),
		filepath.Join(cwd, "..", "..", "database", "schema.sql"),
		"/app/database/schema.sql",
	}

	var schemaPath string
	var schemaSQL []byte
	for _, p := range candidates {
		if p == "" {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(b)) == "" {
			continue
		}
		schemaPath = p
		schemaSQL = b
		break
	}

	if schemaPath == "" {
		return fmt.Errorf("database schema.sql not found (set DATABASE_SCHEMA_PATH)")
	}

	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, string(schemaSQL)); err != nil {
		return fmt.Errorf("failed to apply schema from %s: %w", schemaPath, err)
	}

	return tx.Commit()
}

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize database
	database, err := db.New(cfg.DatabaseURL, cfg.MaxOpenConns, cfg.MaxIdleConns)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer database.Close()

	log.Println("connected to database")

	if errDb := applyDatabaseSchema(context.Background(), database); errDb != nil {
		log.Fatalf("failed to apply database schema: %v", errDb)
	}

	// Initialize HTTP clients
	httpClient, err := httpclient.New(cfg)
	if err != nil {
		log.Fatalf("failed to create HTTP client: %v", err)
	}

	fnetClient := httpclient.NewFnetClient(cfg)

	// Initialize collector registry
	registry := collectors.NewRegistry()
	registry.Register(collectors.NewFundListCollector(httpClient))
	registry.Register(collectors.NewFundDetailsCollector(httpClient))
	registry.Register(collectors.NewIndicatorsCollector(httpClient, database))
	registry.Register(collectors.NewCotationsTodayCollector(httpClient))
	registry.Register(collectors.NewCotationsCollector(httpClient, database))
	registry.Register(collectors.NewDocumentsCollector(fnetClient, database))

	log.Printf("registered %d collectors\n", len(registry.List()))

	// Initialize persister
	persister := persistence.New(database, cfg.Mode)

	// Create work channel with small buffer
	workChan := make(chan scheduler.WorkItem, 20)

	// Initialize scheduler
	sched := scheduler.New(cfg, database, registry, persister, workChan)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if updated, err := persister.RecomputeDividendYields(ctx); err != nil {
		log.Printf("[startup] dividend yield recompute error: %v\n", err)
	} else {
		log.Printf("[startup] dividend yields updated=%d\n", updated)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerPoolSize; i++ {
		wg.Add(1)
		w := worker.New(i+1, registry, persister, workChan, cfg.Mode)
		go func() {
			defer wg.Done()
			if err := w.Start(ctx); err != nil && err != context.Canceled {
				log.Printf("worker error: %v\n", err)
			}
		}()
	}

	// Start scheduler
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sched.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("scheduler error: %v\n", err)
		}
	}()

	log.Printf("go-worker started with %d workers\n", cfg.WorkerPoolSize)

	if interval := statsInterval(); interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					s := worker.Stats()
					inFlightPeak := worker.InFlightPeakAndReset()
					queuePeak := scheduler.QueueLenPeakAndReset()
					enqueued := scheduler.EnqueuedTotal()
					log.Printf(
						"[stats] in_flight=%d processed=%d errors=%d queue_len=%d in_flight_peak=%d queue_peak=%d enqueued=%d",
						s.InFlight,
						s.Processed,
						s.Errors,
						len(workChan),
						inFlightPeak,
						queuePeak,
						enqueued,
					)
				}
			}
		}()
	}

	// Wait for shutdown signal
	<-sigChan
	log.Println("shutdown signal received, stopping...")

	// Cancel context to stop all goroutines
	cancel()

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("shutdown complete")
}
