package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/analytics"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// Persister handles data persistence to PostgreSQL
type Persister struct {
	db                *db.DB
	skipDividendYield bool
}

const cotationPriceScale = 10000

func toPriceInt(price float64) (int, bool) {
	if price <= 0 || !isFiniteFloat(price) {
		return 0, false
	}
	v := int(math.Round(price * cotationPriceScale))
	if v <= 0 {
		return 0, false
	}
	return v, true
}

func fromPriceInt(priceInt int) float64 {
	if priceInt <= 0 {
		return 0
	}
	return float64(priceInt) / float64(cotationPriceScale)
}

func isFiniteFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func (p *Persister) markMetricsDirtyTx(ctx context.Context, tx *sql.Tx, fundCode string) error {
	code := strings.TrimSpace(fundCode)
	if code == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO metrics_dirty (fund_code, updated_at)
		VALUES ($1, NOW())
		ON CONFLICT (fund_code) DO UPDATE SET
			updated_at = NOW()
	`, code)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO fund_state (fund_code, last_metrics_at, created_at, updated_at)
		VALUES ($1, NULL, NOW(), NOW())
		ON CONFLICT (fund_code) DO UPDATE SET
			last_metrics_at = NULL,
			updated_at = NOW()
	`, code)
	return err
}

// New creates a new persister
func New(database *db.DB, mode string) *Persister {
	return &Persister{
		db:                database,
		skipDividendYield: mode == "backfill",
	}
}

// PersistFundList persists fund list data
func (p *Persister) PersistFundList(ctx context.Context, items []collectors.FundListItem) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO fund_master (
			code, sector, p_vp, dividend_yield, dividend_yield_last_5_years,
			daily_liquidity, net_worth, type, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
		ON CONFLICT (code) DO UPDATE SET
			sector = EXCLUDED.sector,
			p_vp = EXCLUDED.p_vp,
			dividend_yield = EXCLUDED.dividend_yield,
			dividend_yield_last_5_years = EXCLUDED.dividend_yield_last_5_years,
			daily_liquidity = EXCLUDED.daily_liquidity,
			net_worth = EXCLUDED.net_worth,
			type = EXCLUDED.type,
			updated_at = NOW()
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		_, err := stmt.ExecContext(ctx,
			item.Code, item.Sector, item.PVP, item.DividendYield,
			item.DividendYieldLast5Years, item.DailyLiquidity,
			item.NetWorth, item.Type,
		)
		if err != nil {
			return fmt.Errorf("failed to insert fund %s: %w", item.Code, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

// PersistFundDetails persists fund details and dividends
func (p *Persister) PersistFundDetails(ctx context.Context, fundCode string, data collectors.FundDetailsData) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	details := data.Details

	// Upsert fund details
	_, err = tx.ExecContext(ctx, `
		INSERT INTO fund_master (
			code, id, cnpj, razao_social, publico_alvo, mandato, segmento,
			tipo_fundo, prazo_duracao, tipo_gestao, taxa_adminstracao,
			daily_liquidity, vacancia, numero_cotistas, cotas_emitidas,
			valor_patrimonial_cota, valor_patrimonial, ultimo_rendimento, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW(), NOW())
		ON CONFLICT (code) DO UPDATE SET
			id = EXCLUDED.id,
			cnpj = EXCLUDED.cnpj,
			razao_social = EXCLUDED.razao_social,
			publico_alvo = EXCLUDED.publico_alvo,
			mandato = EXCLUDED.mandato,
			segmento = EXCLUDED.segmento,
			tipo_fundo = EXCLUDED.tipo_fundo,
			prazo_duracao = EXCLUDED.prazo_duracao,
			tipo_gestao = EXCLUDED.tipo_gestao,
			taxa_adminstracao = EXCLUDED.taxa_adminstracao,
			daily_liquidity = EXCLUDED.daily_liquidity,
			vacancia = EXCLUDED.vacancia,
			numero_cotistas = EXCLUDED.numero_cotistas,
			cotas_emitidas = EXCLUDED.cotas_emitidas,
			valor_patrimonial_cota = EXCLUDED.valor_patrimonial_cota,
			valor_patrimonial = EXCLUDED.valor_patrimonial,
			ultimo_rendimento = EXCLUDED.ultimo_rendimento,
			updated_at = NOW()
	`,
		fundCode, details.ID, details.CNPJ, details.RazaoSocial,
		details.PublicoAlvo, details.Mandato, details.Segmento,
		details.TipoFundo, details.PrazoDuracao, details.TipoGestao,
		details.TaxaAdministracao, details.DailyLiquidity, details.Vacancia,
		details.NumeroCotistas, details.CotasEmitidas, details.ValorPatrimonialCota,
		details.ValorPatrimonial, details.UltimoRendimento,
	)
	if err != nil {
		return fmt.Errorf("failed to persist fund details: %w", err)
	}

	// Persist dividends
	if len(data.Dividends) > 0 {
		cutoffISO := time.Now().UTC().AddDate(-5, 0, 0).Format("2006-01-02")

		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO dividend (fund_code, date_iso, payment, type, value, yield)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (fund_code, date_iso, type) DO UPDATE SET
				payment = EXCLUDED.payment,
				value = EXCLUDED.value,
				yield = EXCLUDED.yield
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare dividend statement: %w", err)
		}
		defer stmt.Close()

		priceByDate := make(map[string]float64, len(data.Dividends))
		for _, div := range data.Dividends {
			typeCode, convErr := strconv.Atoi(strings.TrimSpace(div.Type))
			if convErr != nil || typeCode <= 0 {
				continue
			}
			if div.DateISO != "" && div.DateISO < cutoffISO {
				continue
			}
			if div.Payment != "" && div.Payment < cutoffISO {
				continue
			}

			yield := 0.0

			if !p.skipDividendYield && typeCode == 1 {
				price, ok := priceByDate[div.DateISO]
				if !ok {
					var p float64
					var pInt int
					rowErr := tx.QueryRowContext(
						ctx,
						`SELECT price_int FROM cotation WHERE fund_code = $1 AND date_iso = $2`,
						div.FundCode,
						div.DateISO,
					).Scan(&pInt)
					if rowErr != nil {
						if rowErr != sql.ErrNoRows {
							return fmt.Errorf("failed to fetch cotation price: %w", rowErr)
						}
						pInt = 0
					}
					p = fromPriceInt(pInt)
					priceByDate[div.DateISO] = p
					price = p
				}

				yield = calcYield(div.Value, price)
			}

			_, execErr := stmt.ExecContext(ctx, div.FundCode, div.DateISO, div.Payment, typeCode, div.Value, yield)
			if execErr != nil {
				return fmt.Errorf("failed to insert dividend: %w", execErr)
			}
		}
	}

	if err := p.markMetricsDirtyTx(ctx, tx, fundCode); err != nil {
		return fmt.Errorf("failed to mark metrics dirty: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_details_sync_at", time.Now())
}

func calcYield(value, price float64) float64 {
	if value <= 0 || price <= 0 {
		return 0
	}
	return (value / price) * 100
}

func (p *Persister) RecomputeDividendYields(ctx context.Context) (int64, error) {
	res, err := p.db.ExecContext(ctx, `
	UPDATE dividend d
		SET yield = (
		d.value / (
			SELECT (c.price_int::double precision / 10000.0)
			FROM cotation c
			WHERE c.fund_code = d.fund_code
			AND c.date_iso <= d.date_iso
			AND c.price_int > 0
			ORDER BY c.date_iso DESC
			LIMIT 1
		)
		) * 100
		WHERE 
		d.type = 1
		AND
		d.yield = 0
		AND d.value > 0
		AND EXISTS (
			SELECT 1
			FROM cotation c
			WHERE c.fund_code = d.fund_code
			AND c.date_iso <= d.date_iso
			AND c.price_int > 0
		);
	`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PersistIndicators persists fund indicators
func (p *Persister) PersistIndicators(ctx context.Context, data collectors.IndicatorsData) error {
	type indicatorsSnapshotRow struct {
		Year                 int16
		CotasEmitidas        *float64
		NumeroDeCotistas     *float64
		Vacancia             *float64
		ValorPatrimonialCota *float64
		ValorPatrimonial     *float64
		LiquidezDiaria       *float64
		DividendYield        *float64
		PVP                  *float64
		ValorMercado         *float64
	}

	parseAno := func(raw string) (int16, bool) {
		s := strings.TrimSpace(raw)
		if s == "" {
			return 0, false
		}
		if strings.EqualFold(s, "Atual") {
			year := time.Now().In(time.Local).Year()
			if year < 0 || year > 32767 {
				return 0, false
			}
			return int16(year), true
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > 32767 {
			return 0, false
		}
		return int16(n), true
	}

	byYear := make(map[int16]*indicatorsSnapshotRow, 8)
	for key, items := range data.Data {
		for _, it := range items {
			ano, ok := parseAno(it.Year)
			if !ok {
				continue
			}
			row, ok := byYear[ano]
			if !ok {
				row = &indicatorsSnapshotRow{Year: ano}
				byYear[ano] = row
			}
			switch key {
			case "cotas_emitidas":
				row.CotasEmitidas = it.Value
			case "numero_de_cotistas":
				row.NumeroDeCotistas = it.Value
			case "vacancia":
				row.Vacancia = it.Value
			case "valor_patrimonial_cota":
				row.ValorPatrimonialCota = it.Value
			case "valor_patrimonial":
				row.ValorPatrimonial = it.Value
			case "liquidez_diaria":
				row.LiquidezDiaria = it.Value
			case "dividend_yield":
				row.DividendYield = it.Value
			case "pvp":
				row.PVP = it.Value
			case "valor_mercado":
				row.ValorMercado = it.Value
			}
		}
	}

	if len(byYear) == 0 {
		return p.db.UpdateFundStateTimestamp(ctx, data.FundCode, "last_indicators_at", time.Now())
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	toDBFloat := func(v *float64) any {
		if v == nil {
			return nil
		}
		return *v
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO indicators_snapshot (
			fund_code, ano, fetched_at,
			cotas_emitidas, numero_de_cotistas, vacancia,
			valor_patrimonial_cota, valor_patrimonial, liquidez_diaria,
			dividend_yield, pvp, valor_mercado
		) VALUES (
			$1, $2, NOW(),
			$3, $4, $5,
			$6, $7, $8,
			$9, $10, $11
		)
		ON CONFLICT (fund_code, ano) DO UPDATE SET
			fetched_at = NOW(),
			cotas_emitidas = COALESCE(EXCLUDED.cotas_emitidas, indicators_snapshot.cotas_emitidas),
			numero_de_cotistas = COALESCE(EXCLUDED.numero_de_cotistas, indicators_snapshot.numero_de_cotistas),
			vacancia = COALESCE(EXCLUDED.vacancia, indicators_snapshot.vacancia),
			valor_patrimonial_cota = COALESCE(EXCLUDED.valor_patrimonial_cota, indicators_snapshot.valor_patrimonial_cota),
			valor_patrimonial = COALESCE(EXCLUDED.valor_patrimonial, indicators_snapshot.valor_patrimonial),
			liquidez_diaria = COALESCE(EXCLUDED.liquidez_diaria, indicators_snapshot.liquidez_diaria),
			dividend_yield = COALESCE(EXCLUDED.dividend_yield, indicators_snapshot.dividend_yield),
			pvp = COALESCE(EXCLUDED.pvp, indicators_snapshot.pvp),
			valor_mercado = COALESCE(EXCLUDED.valor_mercado, indicators_snapshot.valor_mercado)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare indicators_snapshot statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range byYear {
		cutoffYear := int16(time.Now().In(time.Local).Year() - 5)
		if row.Year < cutoffYear {
			continue
		}

		_, err := stmt.ExecContext(
			ctx,
			data.FundCode,
			row.Year,
			toDBFloat(row.NumeroDeCotistas),
			toDBFloat(row.Vacancia),
			toDBFloat(row.ValorPatrimonialCota),
			toDBFloat(row.ValorPatrimonial),
			toDBFloat(row.LiquidezDiaria),
			toDBFloat(row.DividendYield),
			toDBFloat(row.PVP),
			toDBFloat(row.ValorMercado),
		)
		if err != nil {
			return fmt.Errorf("failed to persist indicators for fund=%s year=%d: %w", data.FundCode, row.Year, err)
		}
	}

	if err := p.markMetricsDirtyTx(ctx, tx, data.FundCode); err != nil {
		return fmt.Errorf("failed to mark metrics dirty: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit indicators_snapshot transaction: %w", err)
	}

	return p.db.UpdateFundStateTimestamp(ctx, data.FundCode, "last_indicators_at", time.Now())
}

func (p *Persister) PersistMarketSnapshot(ctx context.Context, data collectors.MarketSnapshotData) error {
	if data.DateISO == "" || data.Hour == "" || len(data.Items) == 0 {
		return nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	seen := make(map[string]struct{}, len(data.Items))
	codes := make([]string, 0, len(data.Items))
	for _, it := range data.Items {
		if it.FundCode == "" || it.Price <= 0 {
			continue
		}
		if _, ok := seen[it.FundCode]; ok {
			continue
		}
		seen[it.FundCode] = struct{}{}
		codes = append(codes, it.FundCode)
	}
	if len(codes) == 0 {
		return nil
	}

	rows, err := tx.QueryContext(ctx, `SELECT code FROM fund_master WHERE code = ANY($1)`, pq.Array(codes))
	if err != nil {
		return fmt.Errorf("failed to list existing fund_master codes: %w", err)
	}
	allowed := make(map[string]struct{}, len(codes))
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan existing fund_master code: %w", err)
		}
		allowed[code] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to iterate existing fund_master codes: %w", err)
	}
	_ = rows.Close()
	if len(allowed) == 0 {
		return nil
	}

	stmtCotationToday, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation_today (fund_code, date_iso, hour, price_int, fetched_at)
		VALUES ($1, $2, $3::time, $4, NOW())
		ON CONFLICT (fund_code, date_iso, hour) DO UPDATE SET
			price_int = EXCLUDED.price_int,
			fetched_at = NOW()
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare cotation_today statement: %w", err)
	}
	defer stmtCotationToday.Close()

	stmtFundState, err := tx.PrepareContext(ctx, `
		INSERT INTO fund_state (fund_code, last_cotations_today_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
		ON CONFLICT (fund_code) DO UPDATE SET
			last_cotations_today_at = NOW(),
			updated_at = NOW()
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare fund_state statement: %w", err)
	}
	defer stmtFundState.Close()

	for _, it := range data.Items {
		if it.FundCode == "" || it.Price <= 0 {
			continue
		}

		if _, ok := allowed[it.FundCode]; !ok {
			continue
		}

		priceInt, ok := toPriceInt(it.Price)
		if !ok {
			continue
		}
		if _, err := stmtCotationToday.ExecContext(ctx, it.FundCode, data.DateISO, data.Hour, priceInt); err != nil {
			return fmt.Errorf("failed to upsert cotation_today: %w", err)
		}

		if _, err := stmtFundState.ExecContext(ctx, it.FundCode); err != nil {
			return fmt.Errorf("failed to upsert fund_state: %w", err)
		}

		if err := p.markMetricsDirtyTx(ctx, tx, it.FundCode); err != nil {
			return fmt.Errorf("failed to mark metrics dirty: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// PersistHistoricalCotations persists historical cotations
func (p *Persister) PersistHistoricalCotations(ctx context.Context, fundCode string, items []collectors.CotationItem) error {
	if len(items) == 0 {
		return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_historical_cotations_at", time.Now())
	}

	cutoffISO := time.Now().UTC().AddDate(-5, 0, 0).Format("2006-01-02")

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation (fund_code, date_iso, price_int)
		VALUES ($1, $2, $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			price_int = EXCLUDED.price_int
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	maxDateISO := ""
	insertedAny := false
	for _, item := range items {
		if item.DateISO == "" || item.DateISO < cutoffISO {
			continue
		}
		priceInt, ok := toPriceInt(item.Price)
		if !ok {
			continue
		}
		if maxDateISO == "" || item.DateISO > maxDateISO {
			maxDateISO = item.DateISO
		}
		_, err := stmt.ExecContext(ctx, item.FundCode, item.DateISO, priceInt)
		if err != nil {
			return fmt.Errorf("failed to insert cotation: %w", err)
		}
		insertedAny = true
	}

	if maxDateISO != "" {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO fund_state (fund_code, last_cotation_date_iso, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
			ON CONFLICT (fund_code) DO UPDATE SET
				last_cotation_date_iso = GREATEST(COALESCE(fund_state.last_cotation_date_iso, '1970-01-01'::date), EXCLUDED.last_cotation_date_iso),
				updated_at = NOW()
		`, fundCode, maxDateISO)
		if err != nil {
			return fmt.Errorf("failed to update last cotation date: %w", err)
		}
	}

	if insertedAny {
		if err := p.markMetricsDirtyTx(ctx, tx, fundCode); err != nil {
			return fmt.Errorf("failed to mark metrics dirty: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_historical_cotations_at", time.Now())
}

// PersistDocuments persists fund documents
func (p *Persister) PersistDocuments(ctx context.Context, fundCode string, items []collectors.DocumentItem) error {
	if len(items) == 0 {
		return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_documents_at", time.Now())
	}

	now := time.Now()
	cutoffAt := time.Now().UTC().AddDate(-5, 0, 0)
	maxDocumentID := 0
	hasMaxDocumentID := false
	for _, doc := range items {
		id, err := strconv.Atoi(doc.DocumentID)
		if err != nil || id <= 0 {
			continue
		}
		if !hasMaxDocumentID || id > maxDocumentID {
			maxDocumentID = id
			hasMaxDocumentID = true
		}
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO document (
			fund_code, document_id, title, category, type, date,
			"dateUpload", url, status, version, "send", created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE, NOW())
		ON CONFLICT (fund_code, document_id) DO UPDATE SET
			title = EXCLUDED.title,
			category = EXCLUDED.category,
			type = EXCLUDED.type,
			date = EXCLUDED.date,
			"dateUpload" = EXCLUDED."dateUpload",
			url = EXCLUDED.url,
			status = EXCLUDED.status,
			version = EXCLUDED.version
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	parseDateISOToUTC := func(dateISO string) (time.Time, bool) {
		s := strings.TrimSpace(dateISO)
		if s == "" {
			return time.Time{}, false
		}
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return time.Time{}, false
		}
		return t.UTC(), true
	}

	for _, doc := range items {
		uploadISO := strings.TrimSpace(doc.DateUploadISO)
		if uploadISO == "" {
			uploadISO = parsers.ToDateISO(doc.DateUpload)
		}
		if uploadISO == "" {
			uploadISO = parsers.ToDateISO(doc.Date)
		}
		if uploadISO == "" {
			uploadISO = time.Now().UTC().Format("2006-01-02")
		}

		uploadAt, ok := parseDateISOToUTC(uploadISO)
		if !ok {
			uploadAt = time.Now().UTC()
		}

		dateISO := parsers.ToDateISO(doc.Date)
		if dateISO == "" {
			dateISO = uploadISO
		}
		dateAt, ok := parseDateISOToUTC(dateISO)
		if !ok {
			dateAt = uploadAt
		}

		if uploadAt.Before(cutoffAt) && dateAt.Before(cutoffAt) {
			continue
		}

		_, err := stmt.ExecContext(ctx,
			fundCode, doc.DocumentID, doc.Title, doc.Category, doc.Type,
			dateAt, uploadAt, doc.URL,
			doc.Status, doc.Version,
		)
		if err != nil {
			return fmt.Errorf("failed to insert document: %w", err)
		}
	}

	if hasMaxDocumentID {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO fund_state (fund_code, last_documents_at, last_documents_max_id, created_at, updated_at)
			VALUES ($1, $2, $3, NOW(), NOW())
			ON CONFLICT (fund_code) DO UPDATE SET
				last_documents_at = EXCLUDED.last_documents_at,
				last_documents_max_id = GREATEST(COALESCE(fund_state.last_documents_max_id, 0), EXCLUDED.last_documents_max_id),
				updated_at = NOW()
		`, fundCode, now, maxDocumentID)
		if err != nil {
			return fmt.Errorf("failed to update fund_state documents max id: %w", err)
		}
	} else {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO fund_state (fund_code, created_at, updated_at)
			VALUES ($1, NOW(), NOW())
			ON CONFLICT (fund_code) DO NOTHING
		`, fundCode)
		if err != nil {
			return fmt.Errorf("failed to ensure fund_state row: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE fund_state
			SET last_documents_at = $2, updated_at = NOW()
			WHERE fund_code = $1
		`, fundCode, now)
		if err != nil {
			return fmt.Errorf("failed to update fund_state timestamp: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

func countWeekdaysBetweenInclusive(start time.Time, end time.Time) int {
	a := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	b := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	if b.Before(a) {
		return 0
	}
	count := 0
	for d := a; !d.After(b); d = d.AddDate(0, 0, 1) {
		wd := d.Weekday()
		if wd >= time.Monday && wd <= time.Friday {
			count++
		}
	}
	return count
}

func (p *Persister) DrainDirtyMetrics(ctx context.Context, max int) (int, error) {
	limit := max
	if limit <= 0 {
		limit = 1
	}

	done := 0
	for done < limit {
		var fundCode string
		err := p.db.QueryRowContext(ctx, `
			WITH picked AS (
				SELECT fund_code
				FROM metrics_dirty
				ORDER BY updated_at ASC
				LIMIT 1
			)
			DELETE FROM metrics_dirty d
			USING picked p
			WHERE d.fund_code = p.fund_code
			RETURNING d.fund_code
		`).Scan(&fundCode)
		if err != nil {
			if err == sql.ErrNoRows {
				return done, nil
			}
			return done, err
		}

		if err := p.computeAndUpsertMetrics(ctx, fundCode); err != nil {
			_, _ = p.db.ExecContext(ctx, `
				INSERT INTO metrics_dirty (fund_code, updated_at)
				VALUES ($1, NOW())
				ON CONFLICT (fund_code) DO UPDATE SET
					updated_at = NOW()
			`, fundCode)
			return done, err
		}

		done++
	}

	return done, nil
}

func (p *Persister) RecomputeMetricsForFund(ctx context.Context, fundCode string) error {
	code := strings.TrimSpace(fundCode)
	if code == "" {
		return nil
	}

	if err := p.computeAndUpsertMetrics(ctx, code); err != nil {
		return err
	}

	_, _ = p.db.ExecContext(ctx, `DELETE FROM metrics_dirty WHERE fund_code = $1`, code)
	return nil
}

func (p *Persister) computeAndUpsertMetrics(ctx context.Context, fundCode string) error {
	code := strings.TrimSpace(fundCode)
	if code == "" {
		return nil
	}

	const cotationsLimit = 1825

	rows, err := p.db.QueryContext(ctx, `
		SELECT date_iso, price_int
		FROM cotation
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT $2
	`, code, cotationsLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	type cotRow struct {
		date  time.Time
		price int
	}
	all := make([]cotRow, 0, cotationsLimit)
	for rows.Next() {
		var r cotRow
		if err := rows.Scan(&r.date, &r.price); err != nil {
			return err
		}
		if r.price <= 0 {
			continue
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(all) < 2 {
		return nil
	}

	dates := make([]time.Time, 0, len(all))
	prices := make([]float64, 0, len(all))
	monthLastPrice := map[string]float64{}
	for i := len(all) - 1; i >= 0; i-- {
		d := all[i].date.UTC()
		pf := fromPriceInt(all[i].price)
		if pf <= 0 {
			continue
		}
		dates = append(dates, d)
		prices = append(prices, pf)
		monthLastPrice[d.Format("2006-01")] = pf
	}
	if len(prices) < 2 {
		return nil
	}

	startDate := dates[0]
	endDate := dates[len(dates)-1]
	asOfDateISO := endDate.Format("2006-01-02")

	dailyReturns := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		prev := prices[i-1]
		cur := prices[i]
		if prev > 0 {
			dailyReturns = append(dailyReturns, cur/prev-1)
		}
	}
	meanDailyReturn := analytics.Mean(dailyReturns)
	volDaily := analytics.Stdev(dailyReturns)
	volAnnual := analytics.AnnualizeVolatility(volDaily, 252)
	sharpe := analytics.SharpeRatio(meanDailyReturn, volDaily, 252)
	dd := analytics.ComputeDrawdown(prices)

	last3dReturn := 0.0
	if len(prices) >= 3 && prices[len(prices)-3] > 0 {
		last3dReturn = prices[len(prices)-1]/prices[len(prices)-3] - 1
	}

	expectedTradingDays := countWeekdaysBetweenInclusive(startDate, endDate)
	if expectedTradingDays <= 0 {
		expectedTradingDays = len(prices)
	}
	pctDaysTraded := 0.0
	if expectedTradingDays > 0 {
		pctDaysTraded = float64(len(prices)) / float64(expectedTradingDays)
	}

	monthKeys := make([]string, 0, len(monthLastPrice))
	for k := range monthLastPrice {
		monthKeys = append(monthKeys, k)
	}
	sort.Strings(monthKeys)

	var (
		dividendByMonth          = map[string]float64{}
		dividendValues           = make([]float64, 0, 64)
		dividendPaidMonths12m    = 0
		dividendRegularity12m    = 0.0
		dividendMean12m          = 0.0
		dividendPrevMean11m      = 0.0
		dividendFirstHalfMean12m = 0.0
		dividendLastHalfMean12m  = 0.0
		dividendMax12m           = 0.0
		dividendMin12m           = 0.0
		dividendLastValue        = 0.0
		dividendCV               = 0.0
		dividendTrendSlope       = 0.0
		dyMonthlyMean            = 0.0
	)

	divRows, err := p.db.QueryContext(ctx, `
		SELECT date_iso, payment, type, value
		FROM dividend
		WHERE fund_code = $1
			AND date_iso >= $2
			AND date_iso <= $3
		ORDER BY date_iso ASC
	`, code, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	if err != nil {
		return err
	}
	defer divRows.Close()

	for divRows.Next() {
		var (
			dateISO  time.Time
			payment  time.Time
			typeCode int
			value    float64
		)
		if err := divRows.Scan(&dateISO, &payment, &typeCode, &value); err != nil {
			return err
		}
		if typeCode != 1 || !isFiniteFloat(value) || value <= 0 {
			continue
		}
		dividendValues = append(dividendValues, value)
		mk := ""
		if !dateISO.IsZero() {
			mk = dateISO.UTC().Format("2006-01")
		}
		if mk == "" && !payment.IsZero() {
			mk = payment.UTC().Format("2006-01")
		}
		if mk != "" {
			dividendByMonth[mk] += value
		}
	}
	if err := divRows.Err(); err != nil {
		return err
	}

	divMean := analytics.Mean(dividendValues)
	divStd := analytics.Stdev(dividendValues)
	if divMean > 0 {
		dividendCV = divStd / divMean
	}

	firstMonth := ""
	lastMonth := ""
	if len(monthKeys) > 0 {
		firstMonth = monthKeys[0]
		lastMonth = monthKeys[len(monthKeys)-1]
	}
	allMonths := monthKeys
	if firstMonth != "" && lastMonth != "" {
		allMonths = analytics.ListMonthKeysBetweenInclusive(firstMonth, lastMonth)
	}

	if len(allMonths) > 1 {
		points := make([]analytics.XY, 0, len(allMonths))
		for idx, mk := range allMonths {
			points = append(points, analytics.XY{X: float64(idx), Y: dividendByMonth[mk]})
		}
		dividendTrendSlope = analytics.LinearSlope(points)
	}

	dyByMonth := make([]float64, 0, len(monthKeys))
	for _, mk := range monthKeys {
		div := dividendByMonth[mk]
		price := monthLastPrice[mk]
		if div > 0 && price > 0 {
			dyByMonth = append(dyByMonth, div/price)
		}
	}
	dyMonthlyMean = analytics.Mean(dyByMonth)

	now := time.Now().UTC()
	lastDayPrevMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	endMonth := time.Date(lastDayPrevMonth.Year(), lastDayPrevMonth.Month(), 1, 0, 0, 0, 0, time.UTC)

	months12 := make([]string, 0, 12)
	for i := 11; i >= 0; i-- {
		m := endMonth.AddDate(0, -i, 0)
		months12 = append(months12, m.Format("2006-01"))
	}

	series12 := make([]float64, 0, 12)
	for _, mk := range months12 {
		v := dividendByMonth[mk]
		if v > 0 {
			dividendPaidMonths12m++
		}
		series12 = append(series12, v)
	}

	if dividendPaidMonths12m > 0 {
		dividendRegularity12m = float64(dividendPaidMonths12m) / 12.0
		dividendMean12m = analytics.Mean(series12)
		dividendMax12m = series12[0]
		dividendMin12m = series12[0]
		for _, v := range series12[1:] {
			if v > dividendMax12m {
				dividendMax12m = v
			}
			if v < dividendMin12m {
				dividendMin12m = v
			}
		}
		dividendLastValue = series12[len(series12)-1]

		prev := series12[:len(series12)-1]
		dividendPrevMean11m = analytics.Mean(prev)

		split := len(series12) / 2
		dividendFirstHalfMean12m = analytics.Mean(series12[:split])
		dividendLastHalfMean12m = analytics.Mean(series12[split:])
	}

	pvpCurrent := 0.0
	pvpValues := make([]float64, 0, 8)
	liqValues := make([]float64, 0, 8)
	currentYear := int16(time.Now().In(time.Local).Year())

	indRows, err := p.db.QueryContext(ctx, `
		SELECT ano, pvp, liquidez_diaria
		FROM indicators_snapshot
		WHERE fund_code = $1
		ORDER BY ano ASC
	`, code)
	if err != nil {
		return err
	}
	defer indRows.Close()
	for indRows.Next() {
		var (
			ano            int16
			pvp            sql.NullFloat64
			liquidezDiaria sql.NullFloat64
		)
		if err := indRows.Scan(&ano, &pvp, &liquidezDiaria); err != nil {
			return err
		}
		if pvp.Valid && isFiniteFloat(pvp.Float64) && pvp.Float64 > 0 {
			pvpValues = append(pvpValues, pvp.Float64)
			if ano == currentYear {
				pvpCurrent = pvp.Float64
			}
		}
		if liquidezDiaria.Valid && isFiniteFloat(liquidezDiaria.Float64) && liquidezDiaria.Float64 > 0 {
			liqValues = append(liqValues, liquidezDiaria.Float64)
		}
	}
	if err := indRows.Err(); err != nil {
		return err
	}

	if pvpCurrent <= 0 {
		for i := len(pvpValues) - 1; i >= 0; i-- {
			if pvpValues[i] > 0 {
				pvpCurrent = pvpValues[i]
				break
			}
		}
	}

	var vpc sql.NullFloat64
	if err := p.db.QueryRowContext(ctx, `
		SELECT valor_patrimonial_cota
		FROM fund_master
		WHERE code = $1
		LIMIT 1
	`, code).Scan(&vpc); err != nil {
		if err != sql.ErrNoRows {
			return err
		}
	}
	if pvpCurrent <= 0 && vpc.Valid && vpc.Float64 > 0 && prices[len(prices)-1] > 0 {
		pvpCurrent = prices[len(prices)-1] / vpc.Float64
		pvpValues = append(pvpValues, pvpCurrent)
	}

	pvpPercentile := 0.0
	if len(pvpValues) > 0 && pvpCurrent > 0 {
		pvpPercentile = analytics.PercentileRank(pvpValues, pvpCurrent)
	}

	liqMean := analytics.Mean(liqValues)

	todayReturn := 0.0
	var latestTodayDate time.Time
	err = p.db.QueryRowContext(ctx, `
		SELECT date_iso
		FROM cotation_today
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT 1
	`, code).Scan(&latestTodayDate)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil {
		var firstInt int
		if err := p.db.QueryRowContext(ctx, `
			SELECT price_int
			FROM cotation_today
			WHERE fund_code = $1 AND date_iso = $2
			ORDER BY hour ASC
			LIMIT 1
		`, code, latestTodayDate.Format("2006-01-02")).Scan(&firstInt); err != nil && err != sql.ErrNoRows {
			return err
		}
		var lastInt int
		if err := p.db.QueryRowContext(ctx, `
			SELECT price_int
			FROM cotation_today
			WHERE fund_code = $1 AND date_iso = $2
			ORDER BY hour DESC
			LIMIT 1
		`, code, latestTodayDate.Format("2006-01-02")).Scan(&lastInt); err != nil && err != sql.ErrNoRows {
			return err
		}
		first := fromPriceInt(firstInt)
		last := fromPriceInt(lastInt)
		if first > 0 && last > 0 {
			todayReturn = last/first - 1
		}
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO fund_metrics_latest (
			fund_code, as_of_date, computed_at,
			pvp_current, pvp_percentile, dy_monthly_mean,
			dividend_cv, dividend_trend_slope,
			liq_mean, pct_days_traded,
			vol_annual, sharpe,
			drawdown_max, recovery_time_days,
			today_return, price_last3d_return,
			dividend_paid_months_12m, dividend_regularity_12m,
			dividend_mean_12m, dividend_prev_mean_11m,
			dividend_first_half_mean_12m, dividend_last_half_mean_12m,
			dividend_max_12m, dividend_min_12m, dividend_last_value
		) VALUES (
			$1, $2, NOW(),
			$3, $4, $5,
			$6, $7,
			$8, $9,
			$10, $11,
			$12, $13,
			$14, $15,
			$16, $17,
			$18, $19,
			$20, $21,
			$22, $23, $24
		)
		ON CONFLICT (fund_code) DO UPDATE SET
			as_of_date = EXCLUDED.as_of_date,
			computed_at = NOW(),
			pvp_current = EXCLUDED.pvp_current,
			pvp_percentile = EXCLUDED.pvp_percentile,
			dy_monthly_mean = EXCLUDED.dy_monthly_mean,
			dividend_cv = EXCLUDED.dividend_cv,
			dividend_trend_slope = EXCLUDED.dividend_trend_slope,
			liq_mean = EXCLUDED.liq_mean,
			pct_days_traded = EXCLUDED.pct_days_traded,
			vol_annual = EXCLUDED.vol_annual,
			sharpe = EXCLUDED.sharpe,
			drawdown_max = EXCLUDED.drawdown_max,
			recovery_time_days = EXCLUDED.recovery_time_days,
			today_return = EXCLUDED.today_return,
			price_last3d_return = EXCLUDED.price_last3d_return,
			dividend_paid_months_12m = EXCLUDED.dividend_paid_months_12m,
			dividend_regularity_12m = EXCLUDED.dividend_regularity_12m,
			dividend_mean_12m = EXCLUDED.dividend_mean_12m,
			dividend_prev_mean_11m = EXCLUDED.dividend_prev_mean_11m,
			dividend_first_half_mean_12m = EXCLUDED.dividend_first_half_mean_12m,
			dividend_last_half_mean_12m = EXCLUDED.dividend_last_half_mean_12m,
			dividend_max_12m = EXCLUDED.dividend_max_12m,
			dividend_min_12m = EXCLUDED.dividend_min_12m,
			dividend_last_value = EXCLUDED.dividend_last_value
	`,
		code, asOfDateISO,
		pvpCurrent, pvpPercentile, dyMonthlyMean,
		dividendCV, dividendTrendSlope,
		liqMean, pctDaysTraded,
		volAnnual, sharpe,
		dd.MaxDrawdown, dd.MaxRecoveryDays,
		todayReturn, last3dReturn,
		dividendPaidMonths12m, dividendRegularity12m,
		dividendMean12m, dividendPrevMean11m,
		dividendFirstHalfMean12m, dividendLastHalfMean12m,
		dividendMax12m, dividendMin12m, dividendLastValue,
	)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO fund_state (fund_code, last_cotation_date_iso, last_metrics_at, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW(), NOW())
		ON CONFLICT (fund_code) DO UPDATE SET
			last_cotation_date_iso = GREATEST(COALESCE(fund_state.last_cotation_date_iso, '1970-01-01'::date), EXCLUDED.last_cotation_date_iso),
			last_metrics_at = NOW(),
			updated_at = NOW()
	`, code, asOfDateISO)
	if err != nil {
		return err
	}

	return tx.Commit()
}
