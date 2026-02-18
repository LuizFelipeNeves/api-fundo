package analytics

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

type DrawdownResult struct {
	MaxDrawdown     float64
	MaxRecoveryDays int
}

func ComputeDrawdown(prices []float64) DrawdownResult {
	peak := math.Inf(-1)
	maxDrawdown := 0.0
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
		}

		dd := 0.0
		if peak > 0 {
			dd = price/peak - 1
		}
		if dd < maxDrawdown {
			maxDrawdown = dd
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
			recovery := i - drawdownStartIndex
			if recovery > maxRecovery {
				maxRecovery = recovery
			}
			inDrawdown = false
			currentPeak = price
			currentPeakIndex = i
			continue
		}
	}

	return DrawdownResult{
		MaxDrawdown:     maxDrawdown,
		MaxRecoveryDays: maxRecovery,
	}
}

func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	acc := 0.0
	for _, v := range values {
		acc += v
	}
	return acc / float64(len(values))
}

func Stdev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := Mean(values)
	acc := 0.0
	for _, v := range values {
		d := v - m
		acc += d * d
	}
	return math.Sqrt(acc / float64(len(values)-1))
}

type XY struct {
	X float64
	Y float64
}

func LinearSlope(values []XY) float64 {
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

func AnnualizeVolatility(dailyVolatility float64, tradingDays float64) float64 {
	if !isFinite(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFinite(td) || td <= 0 {
		td = 252
	}
	return dailyVolatility * math.Sqrt(td)
}

func SharpeRatio(meanDailyReturn float64, dailyVolatility float64, tradingDays float64) float64 {
	if !isFinite(meanDailyReturn) {
		return 0
	}
	if !isFinite(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFinite(td) || td <= 0 {
		td = 252
	}
	return (meanDailyReturn / dailyVolatility) * math.Sqrt(td)
}

func PercentileRank(values []float64, current float64) float64 {
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

func MonthKeyToParts(monthKey string) (int, int, bool) {
	parts := strings.Split(strings.TrimSpace(monthKey), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	y, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || m < 1 || m > 12 {
		return 0, 0, false
	}
	return y, m, true
}

func MonthKeyDiff(a string, b string) int {
	ay, am, okA := MonthKeyToParts(a)
	by, bm, okB := MonthKeyToParts(b)
	if !okA || !okB {
		return 0
	}
	return (by-ay)*12 + (bm - am)
}

func MonthKeyAdd(monthKey string, deltaMonths int) string {
	y, m, ok := MonthKeyToParts(monthKey)
	if !ok {
		return ""
	}
	base := y*12 + (m - 1)
	next := base + deltaMonths
	yy := next / 12
	mm := (next % 12) + 1
	return leftPad4(yy) + "-" + leftPad2(mm)
}

func ListMonthKeysBetweenInclusive(startKey string, endKey string) []string {
	diff := MonthKeyDiff(startKey, endKey)
	if diff < 0 {
		return []string{}
	}
	out := make([]string, 0, diff+1)
	for i := 0; i <= diff; i++ {
		k := MonthKeyAdd(startKey, i)
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

func leftPad2(n int) string {
	if n < 10 && n >= 0 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

func leftPad4(n int) string {
	if n >= 0 && n < 10 {
		return "000" + strconv.Itoa(n)
	}
	if n >= 0 && n < 100 {
		return "00" + strconv.Itoa(n)
	}
	if n >= 0 && n < 1000 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
