package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

func Open(ctx context.Context, databaseURL string, poolMax int) (*DB, error) {
	url := strings.TrimSpace(databaseURL)
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	d, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	max := poolMax
	if max <= 0 {
		max = 2
	}

	d.SetMaxOpenConns(max)
	d.SetMaxIdleConns(max)
	d.SetConnMaxLifetime(5 * time.Minute)
	d.SetConnMaxIdleTime(30 * time.Second)

	pingCtx := ctx
	var cancel func()
	if pingCtx == nil {
		pingCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	} else {
		pingCtx, cancel = context.WithTimeout(pingCtx, 5*time.Second)
	}
	defer cancel()

	if err := d.PingContext(pingCtx); err != nil {
		_ = d.Close()
		return nil, err
	}

	return &DB{DB: d}, nil
}
