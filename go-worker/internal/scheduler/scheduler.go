package scheduler

import (
	"context"
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
	switch s.cfg.Mode {
	case config.ModeBackfill:
		return s.startBackfill(ctx)
	default:
		return s.startNormal(ctx)
	}
}
