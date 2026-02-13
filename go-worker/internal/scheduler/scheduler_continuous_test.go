package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
)

func TestIteratorState_PeekIsIdempotentUntilCommit(t *testing.T) {
	now := time.Now()
	it := iteratorState{
		collector: "documents",
		candidates: []db.FundCandidate{
			{Code: "AAA", CNPJ: "1", ID: "x"},
		},
	}

	item1, ok := it.peek(context.Background(), now)
	if !ok {
		t.Fatalf("expected ok")
	}
	item2, ok := it.peek(context.Background(), now)
	if !ok {
		t.Fatalf("expected ok")
	}
	if item1.FundCode != item2.FundCode {
		t.Fatalf("expected same item before commit, got %q and %q", item1.FundCode, item2.FundCode)
	}

	it.commit()

	_, ok = it.peek(context.Background(), now)
	if ok {
		t.Fatalf("expected no item after commit")
	}
}

func TestDispatchGap_SingleTypeIsOneThirdRate(t *testing.T) {
	gapMulti := dispatchGap(2, 0, 20, 3)
	gapSingle := dispatchGap(1, 0, 20, 3)
	if gapSingle != gapMulti*3 {
		t.Fatalf("expected single gap to be 3x multi gap, got %v vs %v", gapSingle, gapMulti)
	}
}

func TestRoundRobinSelection_FairAcrossIterators(t *testing.T) {
	now := time.Now()
	ctx := context.Background()

	iters := []iteratorState{
		{
			collector: "a",
			candidates: []db.FundCandidate{
				{Code: "a1"},
				{Code: "a2"},
			},
		},
		{
			collector: "b",
			candidates: []db.FundCandidate{
				{Code: "b1"},
				{Code: "b2"},
			},
		},
		{
			collector: "c",
			candidates: []db.FundCandidate{
				{Code: "c1"},
				{Code: "c2"},
			},
		},
	}

	rrIndex := 0
	var got []string

	for len(got) < 6 {
		dispatched := false
		for step := 0; step < len(iters); step++ {
			i := rrIndex + step
			if i >= len(iters) {
				i -= len(iters)
			}

			item, ok := iters[i].peek(ctx, now)
			if !ok {
				continue
			}

			iters[i].commit()
			rrIndex = i + 1
			if rrIndex >= len(iters) {
				rrIndex = 0
			}

			got = append(got, item.CollectorName)
			dispatched = true
			break
		}
		if !dispatched {
			break
		}
	}

	want := []string{"a", "b", "c", "a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("expected %d items, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d expected %q, got %q (full=%v)", i, want[i], got[i], got)
		}
	}
}
