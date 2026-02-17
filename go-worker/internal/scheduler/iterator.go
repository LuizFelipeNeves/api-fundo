package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
)

type WorkItem struct {
	CollectorName string
	FundCode      string
	CNPJ          string
	ID            string
}

type iteratorState struct {
	collector        string
	refillInterval   time.Duration
	phase            time.Duration
	nextRefill       time.Time
	enabled          func(now time.Time) bool
	refill           func(ctx context.Context) ([]db.FundCandidate, error)
	candidates       []db.FundCandidate
	candidateIndex   int
	singletonPending bool
}

func (it *iteratorState) isSingleton() bool {
	switch it.collector {
	case "fund_list", "market_snapshot":
		return true
	default:
		return false
	}
}

func nextRefillWithPhase(now time.Time, interval, phase time.Duration) time.Time {
	if interval <= 0 {
		return now
	}

	if phase < 0 {
		phase = 0
	}
	if phase >= interval {
		phase = phase % interval
	}

	shifted := now.Add(-phase)
	next := shifted.Truncate(interval).Add(interval).Add(phase)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

func (it *iteratorState) hasBuffered() bool {
	if it.isSingleton() {
		return it.singletonPending
	}
	return it.candidateIndex < len(it.candidates)
}

func (it *iteratorState) peek(ctx context.Context, now time.Time) (WorkItem, bool) {
	if it.enabled != nil && !it.enabled(now) {
		it.candidates = nil
		it.candidateIndex = 0
		it.singletonPending = false
		return WorkItem{}, false
	}

	if it.isSingleton() {
		if !it.singletonPending {
			if it.nextRefill.IsZero() || !now.Before(it.nextRefill) {
				it.singletonPending = true
				it.nextRefill = nextRefillWithPhase(now, it.refillInterval, it.phase)
			} else {
				return WorkItem{}, false
			}
		}

		return WorkItem{CollectorName: it.collector}, true
	}

	if it.candidateIndex < len(it.candidates) {
		c := it.candidates[it.candidateIndex]
		return WorkItem{
			CollectorName: it.collector,
			FundCode:      c.Code,
			CNPJ:          c.CNPJ,
			ID:            c.ID,
		}, true
	}

	if !it.nextRefill.IsZero() && now.Before(it.nextRefill) {
		return WorkItem{}, false
	}

	if it.refill == nil {
		return WorkItem{}, false
	}

	candidates, err := it.refill(ctx)
	if err != nil {
		log.Println("[scheduler]", it.collector, "refill error:", err)
		it.nextRefill = now.Add(5 * time.Second)
		return WorkItem{}, false
	}

	it.nextRefill = nextRefillWithPhase(now, it.refillInterval, it.phase)
	it.candidateIndex = 0
	it.candidates = candidates

	if len(it.candidates) == 0 {
		return WorkItem{}, false
	}

	c := it.candidates[it.candidateIndex]
	return WorkItem{
		CollectorName: it.collector,
		FundCode:      c.Code,
		CNPJ:          c.CNPJ,
		ID:            c.ID,
	}, true
}

func (it *iteratorState) commit() {
	if it.isSingleton() {
		it.singletonPending = false
		return
	}

	if it.candidateIndex < len(it.candidates) {
		it.candidateIndex++
		if it.candidateIndex >= len(it.candidates) {
			it.candidates = nil
			it.candidateIndex = 0
		}
	}
}
