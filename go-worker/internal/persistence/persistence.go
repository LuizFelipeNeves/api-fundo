package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
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
			daily_liquidity, net_worth, type, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
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
			valor_patrimonial_cota, valor_patrimonial, ultimo_rendimento, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW())
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
		SET yield = (d.value / c.price) * 100
		FROM cotation c
		WHERE d.fund_code = c.fund_code
			AND d.date_iso = c.date_iso
			AND c.price > 0
			AND d.yield = 0
	`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// PersistIndicators persists fund indicators
func (p *Persister) PersistIndicators(ctx context.Context, data collectors.IndicatorsData) error {
	dataJSON, err := json.Marshal(data.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal indicators: %w", err)
	}

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO indicators_snapshot (fund_code, fetched_at, data_hash, data_json)
		VALUES ($1, NOW(), $2, $3)
		ON CONFLICT (fund_code) DO UPDATE SET
			fetched_at = NOW(),
			data_hash = EXCLUDED.data_hash,
			data_json = EXCLUDED.data_json
	`, data.FundCode, data.DataHash, dataJSON)
	if err != nil {
		return fmt.Errorf("failed to persist indicators: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, data.FundCode, "last_indicators_at", time.Now())
}

// PersistCotationsToday persists today's cotations snapshot
func (p *Persister) PersistCotationsToday(ctx context.Context, data collectors.CotationsTodayData) error {
	dataJSON, err := json.Marshal(data.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal cotations: %w", err)
	}

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO cotations_today_snapshot (fund_code, date_iso, fetched_at, data_json)
		VALUES ($1, $2, NOW(), $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			fetched_at = NOW(),
			data_json = EXCLUDED.data_json
	`, data.FundCode, data.DateISO, dataJSON)
	if err != nil {
		return fmt.Errorf("failed to persist cotations today: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, data.FundCode, "last_cotations_today_at", time.Now())
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
			date_upload_iso, "dateUpload", url, status, version
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (fund_code, document_id) DO UPDATE SET
			title = EXCLUDED.title,
			category = EXCLUDED.category,
			type = EXCLUDED.type,
			date = EXCLUDED.date,
			date_upload_iso = EXCLUDED.date_upload_iso,
			"dateUpload" = EXCLUDED."dateUpload",
			url = EXCLUDED.url,
			status = EXCLUDED.status,
			version = EXCLUDED.version
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, doc := range items {
		_, err := stmt.ExecContext(ctx,
			doc.FundCode, doc.DocumentID, doc.Title, doc.Category, doc.Type,
			doc.Date, doc.DateUploadISO, doc.DateUpload, doc.URL,
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
