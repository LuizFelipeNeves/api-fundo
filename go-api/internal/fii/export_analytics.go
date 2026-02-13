package fii

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type drawdownResult struct {
	MaxDrawdown     float64
	PeakIndex       int
	TroughIndex     int
	MaxDurationDays int
	MaxRecoveryDays int
}

func computeDrawdown(prices []float64) drawdownResult {
	peak := math.Inf(-1)
	maxDrawdown := 0.0
	peakIndex := 0
	troughIndex := 0
	maxDuration := 0
	maxRecovery := 0

	currentPeakIndex := 0
	currentPeak := 0.0
	if len(prices) > 0 {
		currentPeak = prices[0]
	}
	peak = currentPeak

	inDrawdown := false
	drawdownStartIndex := 0
	drawdownPeakValue := currentPeak

	for i := 0; i < len(prices); i++ {
		price := prices[i]
		if price <= 0 {
			continue
		}

		if price > peak {
			peak = price
			peakIndex = i
		}

		dd := 0.0
		if peak > 0 {
			dd = price/peak - 1
		}
		if dd < maxDrawdown {
			maxDrawdown = dd
			troughIndex = i
		}

		if !inDrawdown {
			if price < currentPeak && currentPeak > 0 {
				inDrawdown = true
				drawdownStartIndex = currentPeakIndex
				drawdownPeakValue = currentPeak
			} else if price >= currentPeak {
				currentPeak = price
				currentPeakIndex = i
			}
			continue
		}

		if price >= drawdownPeakValue {
			duration := i - drawdownStartIndex
			if duration > maxDuration {
				maxDuration = duration
			}
			if duration > maxRecovery {
				maxRecovery = duration
			}
			inDrawdown = false
			currentPeak = price
			currentPeakIndex = i
			continue
		}
	}

	if inDrawdown && len(prices) > 0 {
		duration := (len(prices) - 1) - drawdownStartIndex
		if duration > maxDuration {
			maxDuration = duration
		}
	}

	return drawdownResult{
		MaxDrawdown:     maxDrawdown,
		PeakIndex:       peakIndex,
		TroughIndex:     troughIndex,
		MaxDurationDays: maxDuration,
		MaxRecoveryDays: maxRecovery,
	}
}

type seriesPoint struct {
	At    string
	Value float64
}

func pickIndicatorValue(data model.NormalizedIndicators, key string) (float64, bool) {
	series := data[key]
	if len(series) == 0 {
		return 0, false
	}
	var candidate *float64
	for i := range series {
		if strings.TrimSpace(series[i].Year) == "Atual" {
			candidate = series[i].Value
			break
		}
	}
	if candidate == nil {
		candidate = series[0].Value
	}
	if candidate == nil || !isFiniteFloat(*candidate) {
		return 0, false
	}
	return *candidate, true
}

func parseISOTime(value string) (time.Time, bool) {
	v := strings.TrimSpace(value)
	if v == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func computeIndicatorSeries(snapshots []IndicatorsSnapshot, key string) []seriesPoint {
	out := make([]seriesPoint, 0, len(snapshots))
	for _, s := range snapshots {
		v, ok := pickIndicatorValue(s.Data, key)
		if !ok {
			continue
		}
		out = append(out, seriesPoint{At: s.FetchedAt, Value: v})
	}
	sort.Slice(out, func(i, j int) bool {
		a, okA := parseISOTime(out[i].At)
		b, okB := parseISOTime(out[j].At)
		if okA && okB {
			return a.Before(b)
		}
		return out[i].At < out[j].At
	})
	return out
}

func computeGrowth(series []seriesPoint, daysBack int) float64 {
	if len(series) < 2 {
		return 0
	}
	last := series[len(series)-1]
	lastAt, ok := parseISOTime(last.At)
	if !ok || last.Value <= 0 {
		return 0
	}
	target := lastAt.Add(-time.Duration(daysBack) * 24 * time.Hour)
	var base *seriesPoint
	for i := len(series) - 1; i >= 0; i-- {
		at, ok := parseISOTime(series[i].At)
		if ok && (at.Before(target) || at.Equal(target)) {
			base = &series[i]
			break
		}
	}
	if base == nil || base.Value <= 0 {
		return 0
	}
	return last.Value/base.Value - 1
}

func percentileRank(values []float64, current float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	count := 0
	for _, v := range sorted {
		if v <= current {
			count++
		}
	}
	return float64(count) / float64(len(sorted))
}
