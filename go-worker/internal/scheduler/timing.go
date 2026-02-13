package scheduler

import (
	"context"
	"time"
)

func dispatchGap(activeTypes, queued, capacity, workerPoolSize int) time.Duration {
	if capacity <= 0 {
		return 50 * time.Millisecond
	}

	if workerPoolSize <= 0 {
		workerPoolSize = 1
	}

	lowWater := workerPoolSize
	highWater := workerPoolSize * 2
	if highWater > capacity-1 {
		highWater = capacity - 1
	}
	if highWater < lowWater {
		highWater = lowWater
	}

	gap := 40 * time.Millisecond
	if queued < lowWater {
		gap = 20 * time.Millisecond
	}
	if queued >= highWater {
		gap = 120 * time.Millisecond
	}
	if queued >= capacity-1 {
		gap = 250 * time.Millisecond
	}

	if activeTypes <= 1 {
		gap *= 3
	}

	return gap
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextWake(now time.Time, iters []iteratorState) time.Time {
	next := time.Time{}
	for i := range iters {
		it := &iters[i]
		if it.hasBuffered() {
			return now
		}
		if it.nextRefill.IsZero() {
			return now
		}
		if next.IsZero() || it.nextRefill.Before(next) {
			next = it.nextRefill
		}
	}
	if next.IsZero() {
		return now.Add(250 * time.Millisecond)
	}
	return next
}

func countActiveTypes(iters []iteratorState) int {
	active := 0
	now := time.Now()

	for i := range iters {
		if iters[i].enabled != nil && !iters[i].enabled(now) {
			continue
		}
		active++
	}

	if active == 0 {
		return 1
	}
	return active
}
