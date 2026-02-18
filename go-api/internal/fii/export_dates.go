package fii

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func ToDateISOFromBR(dateBr string) string {
	v := strings.TrimSpace(dateBr)
	if v == "" {
		return ""
	}
	t, err := time.Parse("02/01/2006", v)
	if err != nil {
		return ""
	}
	return t.Format("2006-01-02")
}

func toMonthKeyFromBr(dateBr string) string {
	iso := ToDateISOFromBR(dateBr)
	if len(iso) < 7 {
		return ""
	}
	return iso[:7]
}

func monthKeyToParts(monthKey string) (int, int, bool) {
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

func monthKeyDiff(a string, b string) int {
	ay, am, okA := monthKeyToParts(a)
	by, bm, okB := monthKeyToParts(b)
	if !okA || !okB {
		return 0
	}
	return (by-ay)*12 + (bm - am)
}

func monthKeyAdd(monthKey string, deltaMonths int) string {
	y, m, ok := monthKeyToParts(monthKey)
	if !ok {
		return ""
	}
	base := y*12 + (m - 1)
	next := base + deltaMonths
	yy := next / 12
	mm := (next % 12) + 1
	return leftPad4(yy) + "-" + leftPad2(mm)
}

func leftPad2(v int) string {
	return fmt.Sprintf("%02d", v)
}

func leftPad4(v int) string {
	return fmt.Sprintf("%04d", v)
}

func listMonthKeysBetweenInclusive(startKey string, endKey string) []string {
	diff := monthKeyDiff(startKey, endKey)
	if diff < 0 {
		return []string{}
	}
	out := make([]string, 0, diff+1)
	for i := 0; i <= diff; i++ {
		k := monthKeyAdd(startKey, i)
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

func countWeekdaysBetweenIso(startIso string, endIso string) int {
	start, err1 := time.Parse("2006-01-02", strings.TrimSpace(startIso))
	end, err2 := time.Parse("2006-01-02", strings.TrimSpace(endIso))
	if err1 != nil || err2 != nil || end.Before(start) {
		return 0
	}

	count := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		wd := d.Weekday()
		if wd >= time.Monday && wd <= time.Friday {
			count++
		}
	}
	return count
}
