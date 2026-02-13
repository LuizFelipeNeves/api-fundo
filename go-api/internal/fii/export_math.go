package fii

import (
	"math"
	"sort"
)

func clampInt(v int, fallback int, min int, max int) int {
	if v <= 0 {
		return fallback
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func isFiniteFloat(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	acc := 0.0
	for _, v := range values {
		acc += v
	}
	return acc / float64(len(values))
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func stdev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := mean(values)
	acc := 0.0
	for _, v := range values {
		d := v - m
		acc += d * d
	}
	return math.Sqrt(acc / float64(len(values)-1))
}

func quantile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	q := p
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	idx := float64(len(sorted)-1) * q
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	w := idx - float64(lo)
	return sorted[lo]*(1-w) + sorted[hi]*w
}

type xy struct {
	X float64
	Y float64
}

func linearSlope(values []xy) float64 {
	n := len(values)
	if n < 2 {
		return 0
	}
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumXX := 0.0
	for _, p := range values {
		sumX += p.X
		sumY += p.Y
		sumXY += p.X * p.Y
		sumXX += p.X * p.X
	}
	denom := float64(n)*sumXX - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (float64(n)*sumXY - sumX*sumY) / denom
}

func annualizeVolatility(dailyVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return dailyVolatility * math.Sqrt(td)
}

func annualizeCagr(simpleReturn float64, periodDays int) float64 {
	if !isFiniteFloat(simpleReturn) || simpleReturn <= -1 {
		return 0
	}
	if periodDays <= 0 {
		return 0
	}
	return math.Pow(1+simpleReturn, 365/float64(periodDays)) - 1
}

func sharpeRatio(meanDailyReturn float64, dailyVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(meanDailyReturn) {
		return 0
	}
	if !isFiniteFloat(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return (meanDailyReturn / dailyVolatility) * math.Sqrt(td)
}

func sortinoRatio(meanDailyReturn float64, downsideVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(meanDailyReturn) {
		return 0
	}
	if !isFiniteFloat(downsideVolatility) || downsideVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return (meanDailyReturn / downsideVolatility) * math.Sqrt(td)
}

func calmarRatio(cagrAnnualized float64, maxDrawdown float64) float64 {
	if !isFiniteFloat(cagrAnnualized) {
		return 0
	}
	if !isFiniteFloat(maxDrawdown) {
		return 0
	}
	dd := math.Abs(maxDrawdown)
	if dd <= 0 {
		return 0
	}
	return cagrAnnualized / dd
}

func rN(v float64, decimals int) float64 {
	if !isFiniteFloat(v) {
		return 0
	}
	if decimals <= 0 {
		return math.Round(v)
	}
	pow := math.Pow10(decimals)
	return math.Round(v*pow) / pow
}

func r2(v float64) float64 { return rN(v, 2) }
func r4(v float64) float64 { return rN(v, 4) }
func r6(v float64) float64 { return rN(v, 6) }

func clamp01(v float64) float64 {
	if !isFiniteFloat(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
