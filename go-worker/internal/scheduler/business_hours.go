package scheduler

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"
)

func (s *Scheduler) scheduleEODCotation(ctx context.Context, dateISO string) {
	lockKey := int64(4419270101)

	err := s.db.TryAdvisoryLock(ctx, lockKey, func(tx *sql.Tx) error {
		log.Println("[scheduler] processing EOD cotation")
		inserted, err := runEODCotation(ctx, tx, dateISO)
		if err != nil {
			return err
		}
		log.Printf("[scheduler] EOD cotation done inserted=%d\n", inserted)
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
	return total >= 1140 && total <= 1150
}
