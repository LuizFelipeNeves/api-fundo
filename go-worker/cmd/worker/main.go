package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/persistence"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/scheduler"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/worker"
)

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

	// Initialize collector registry
	registry := collectors.NewRegistry()
	registry.Register(collectors.NewFundListCollector())
	registry.Register(collectors.NewFundDetailsCollector())
	registry.Register(collectors.NewIndicatorsCollector())
	registry.Register(collectors.NewCotationsTodayCollector())
	registry.Register(collectors.NewCotationsCollector())
	registry.Register(collectors.NewDocumentsCollector())

	log.Printf("registered %d collectors\n", len(registry.All()))

	// Initialize persister
	persister := persistence.New(database)

	// Create work channel with small buffer
	workChan := make(chan scheduler.WorkItem, 20)

	// Initialize scheduler
	sched := scheduler.New(cfg, database, registry, persister, workChan)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerPoolSize; i++ {
		wg.Add(1)
		w := worker.New(i+1, registry, persister, workChan)
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

	// Wait for shutdown signal
	<-sigChan
	log.Println("shutdown signal received, stopping...")

	// Cancel context to stop all goroutines
	cancel()

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("shutdown complete")
}
