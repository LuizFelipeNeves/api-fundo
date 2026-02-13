package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type todayPoint struct {
	Price float64         `json:"price"`
	Hour  json.RawMessage `json:"hour"`
}

type todayWrapper struct {
	Real []todayPoint `json:"real"`
}

var hhmmRe = regexp.MustCompile(`\b(\d{2}):(\d{2})\b`)

func runEODCotation(ctx context.Context, tx *sql.Tx, dateISO string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT fund_code, data_json
		FROM cotations_today_snapshot
		WHERE date_iso = $1
	`, dateISO)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation (fund_code, date_iso, price)
		VALUES ($1, $2, $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			price = EXCLUDED.price
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	inserted := 0
	for rows.Next() {
		var fundCode string
		var dataJSON []byte
		if err := rows.Scan(&fundCode, &dataJSON); err != nil {
			return inserted, err
		}

		price, ok := latestTodayPrice(dataJSON)
		if !ok {
			continue
		}

		if _, err := stmt.ExecContext(ctx, fundCode, dateISO, price); err != nil {
			return inserted, fmt.Errorf("insert cotation fund=%s date=%s: %w", fundCode, dateISO, err)
		}
		inserted++
	}
	if err := rows.Err(); err != nil {
		return inserted, err
	}

	return inserted, nil
}

func latestTodayPrice(dataJSON []byte) (float64, bool) {
	if len(dataJSON) == 0 {
		return 0, false
	}

	var points []todayPoint
	if err := json.Unmarshal(dataJSON, &points); err == nil {
		return pickLatestPrice(points)
	}

	var w todayWrapper
	if err := json.Unmarshal(dataJSON, &w); err == nil && len(w.Real) > 0 {
		return pickLatestPrice(w.Real)
	}

	return 0, false
}

func pickLatestPrice(points []todayPoint) (float64, bool) {
	bestMin := -1
	bestPrice := 0.0
	for i := range points {
		min, ok := parseHourMinutes(points[i].Hour)
		if !ok {
			continue
		}
		if min >= bestMin {
			bestMin = min
			bestPrice = points[i].Price
		}
	}
	if bestMin < 0 || bestPrice <= 0 {
		return 0, false
	}
	return bestPrice, true
}

func parseHourMinutes(raw json.RawMessage) (int, bool) {
	raw = bytesTrimSpace(raw)
	if len(raw) == 0 {
		return 0, false
	}

	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, false
		}
		s = strings.TrimSpace(s)
		return parseHHMMFromString(s)
	}

	var n float64
	if err := json.Unmarshal(raw, &n); err == nil && n > 0 {
		t := time.UnixMilli(int64(n))
		return t.Hour()*60 + t.Minute(), true
	}

	return 0, false
}

func parseHHMMFromString(s string) (int, bool) {
	m := hhmmRe.FindStringSubmatch(s)
	if len(m) != 3 {
		return 0, false
	}
	h, err1 := strconv.Atoi(m[1])
	min, err2 := strconv.Atoi(m[2])
	if err1 != nil || err2 != nil {
		return 0, false
	}
	if h < 0 || h > 23 || min < 0 || min > 59 {
		return 0, false
	}
	return h*60 + min, true
}

func bytesTrimSpace(b []byte) []byte {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
		i++
	}
	j := len(b) - 1
	for j >= i && (b[j] == ' ' || b[j] == '\n' || b[j] == '\r' || b[j] == '\t') {
		j--
	}
	if i == 0 && j == len(b)-1 {
		return b
	}
	if j < i {
		return b[:0]
	}
	return b[i : j+1]
}
