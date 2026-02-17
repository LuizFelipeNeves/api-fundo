package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// Persister handles data persistence to PostgreSQL
type Persister struct {
	db                *db.DB
	skipDividendYield bool
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
			yield := 0.0

			if !p.skipDividendYield {
				price, ok := priceByDate[div.DateISO]
				if !ok {
					var p float64
					err := tx.QueryRowContext(
						ctx,
						`SELECT price FROM cotation WHERE fund_code = $1 AND date_iso = $2`,
						div.FundCode,
						div.DateISO,
					).Scan(&p)
					if err != nil {
						if err != sql.ErrNoRows {
							return fmt.Errorf("failed to fetch cotation price: %w", err)
						}
						p = 0
					}
					priceByDate[div.DateISO] = p
					price = p
				}

				yield = calcYield(div.Value, price)
			}

			_, err := stmt.ExecContext(ctx, div.FundCode, div.DateISO, div.Payment, div.Type, div.Value, yield)
			if err != nil {
				return fmt.Errorf("failed to insert dividend: %w", err)
			}
		}
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
			SELECT c.price
			FROM cotation c
			WHERE c.fund_code = d.fund_code
			AND c.date_iso <= d.date_iso
			AND c.price > 0
			ORDER BY c.date_iso DESC
			LIMIT 1
		)
		) * 100
		WHERE 
		d.yield = 0
		AND d.value > 0
		AND EXISTS (
			SELECT 1
			FROM cotation c
			WHERE c.fund_code = d.fund_code
			AND c.date_iso <= d.date_iso
			AND c.price > 0
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
		_, err := stmt.ExecContext(
			ctx,
			data.FundCode,
			row.Year,
			toDBFloat(row.CotasEmitidas),
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
		INSERT INTO cotation_today (fund_code, date_iso, hour, price, fetched_at)
		VALUES ($1, $2, $3::time, $4, NOW())
		ON CONFLICT (fund_code, date_iso, hour) DO UPDATE SET
			price = EXCLUDED.price,
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

		if _, err := stmtCotationToday.ExecContext(ctx, it.FundCode, data.DateISO, data.Hour, it.Price); err != nil {
			return fmt.Errorf("failed to upsert cotation_today: %w", err)
		}

		if _, err := stmtFundState.ExecContext(ctx, it.FundCode); err != nil {
			return fmt.Errorf("failed to upsert fund_state: %w", err)
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

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cotation (fund_code, date_iso, price)
		VALUES ($1, $2, $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			price = EXCLUDED.price
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		_, err := stmt.ExecContext(ctx, item.FundCode, item.DateISO, item.Price)
		if err != nil {
			return fmt.Errorf("failed to insert cotation: %w", err)
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
