package scheduler

import "sync/atomic"

var (
	queueLenPeak  int64
	enqueuedTotal int64
)

func EnqueuedTotal() int64 {
	return atomic.LoadInt64(&enqueuedTotal)
}

func QueueLenPeakAndReset() int64 {
	return atomic.SwapInt64(&queueLenPeak, 0)
}

func bumpPeakInt(dst *int64, v int) {
	if v <= 0 {
		return
	}
	val := int64(v)
	for {
		cur := atomic.LoadInt64(dst)
		if val <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(dst, cur, val) {
			return
		}
	}
}
