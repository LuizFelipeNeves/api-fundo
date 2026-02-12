package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/collectors"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
)

// Persister handles data persistence to PostgreSQL
type Persister struct {
	db *db.DB
}

// New creates a new persister
func New(database *db.DB) *Persister {
	return &Persister{db: database}
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

	return tx.Commit()
}

// PersistFundDetails persists fund details
func (p *Persister) PersistFundDetails(ctx context.Context, details collectors.FundDetails) error {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO fund_master (
			code, id, cnpj, razao_social, publico_alvo, mandato, segmento,
			tipo_fundo, prazo_duracao, tipo_gestao, taxa_adminstracao,
			vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
			valor_patrimonial, ultimo_rendimento, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
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
			vacancia = EXCLUDED.vacancia,
			numero_cotistas = EXCLUDED.numero_cotistas,
			cotas_emitidas = EXCLUDED.cotas_emitidas,
			valor_patrimonial_cota = EXCLUDED.valor_patrimonial_cota,
			valor_patrimonial = EXCLUDED.valor_patrimonial,
			ultimo_rendimento = EXCLUDED.ultimo_rendimento,
			updated_at = NOW()
	`,
		details.Code, details.ID, details.CNPJ, details.RazaoSocial,
		details.PublicoAlvo, details.Mandato, details.Segmento,
		details.TipoFundo, details.PrazoDuracao, details.TipoGestao,
		details.TaxaAdministracao, details.Vacancia, details.NumeroCotistas,
		details.CotasEmitidas, details.ValorPatrimonialCota,
		details.ValorPatrimonial, details.UltimoRendimento,
	)
	if err != nil {
		return fmt.Errorf("failed to persist fund details: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, details.Code, "last_details_sync_at", time.Now())
}

// PersistIndicators persists fund indicators
func (p *Persister) PersistIndicators(ctx context.Context, fundCode string, indicators collectors.Indicators) error {
	dataJSON, err := json.Marshal(indicators)
	if err != nil {
		return fmt.Errorf("failed to marshal indicators: %w", err)
	}

	// Calculate hash for deduplication
	dataHash := fmt.Sprintf("%x", dataJSON) // Simple hash, could use crypto/sha256 for better hashing

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO indicators_snapshot (fund_code, fetched_at, data_hash, data_json)
		VALUES ($1, NOW(), $2, $3)
		ON CONFLICT (fund_code) DO UPDATE SET
			fetched_at = NOW(),
			data_hash = EXCLUDED.data_hash,
			data_json = EXCLUDED.data_json
	`, fundCode, dataHash, dataJSON)
	if err != nil {
		return fmt.Errorf("failed to persist indicators: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_indicators_at", time.Now())
}

// PersistCotationsToday persists today's cotations snapshot
func (p *Persister) PersistCotationsToday(ctx context.Context, fundCode string, cotations collectors.CotationsResponse) error {
	dataJSON, err := json.Marshal(cotations)
	if err != nil {
		return fmt.Errorf("failed to marshal cotations: %w", err)
	}

	dateISO := time.Now().Format("2006-01-02")

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO cotations_today_snapshot (fund_code, date_iso, fetched_at, data_json)
		VALUES ($1, $2, NOW(), $3)
		ON CONFLICT (fund_code, date_iso) DO UPDATE SET
			fetched_at = NOW(),
			data_json = EXCLUDED.data_json
	`, fundCode, dateISO, dataJSON)
	if err != nil {
		return fmt.Errorf("failed to persist cotations today: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_cotations_today_at", time.Now())
}

// PersistHistoricalCotations persists historical cotations
func (p *Persister) PersistHistoricalCotations(ctx context.Context, fundCode string, cotations collectors.CotationsResponse) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert BRL cotations
	for _, entry := range cotations.Real {
		dateISO, err := parseDate(entry.Date)
		if err != nil {
			continue // Skip invalid dates
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO cotation (fund_code, date_iso, price)
			VALUES ($1, $2, $3)
			ON CONFLICT (fund_code, date_iso) DO UPDATE SET
				price = EXCLUDED.price
		`, fundCode, dateISO, entry.Price)
		if err != nil {
			return fmt.Errorf("failed to insert cotation: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_historical_cotations_at", time.Now())
}

// PersistDocuments persists fund documents
func (p *Persister) PersistDocuments(ctx context.Context, fundCode string, documents []collectors.Document) error {
	if len(documents) == 0 {
		// Still update the timestamp even if no documents
		return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_documents_at", time.Now())
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

	for _, doc := range documents {
		_, err := stmt.ExecContext(ctx,
			fundCode, doc.DocumentID, doc.Title, doc.Category, doc.Type,
			doc.Date, doc.DateUploadISO, doc.DateUpload, doc.URL,
			doc.Status, doc.Version,
		)
		if err != nil {
			return fmt.Errorf("failed to insert document %d: %w", doc.DocumentID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update fund_state timestamp
	return p.db.UpdateFundStateTimestamp(ctx, fundCode, "last_documents_at", time.Now())
}

// parseDate parses a date string in DD/MM/YYYY format
func parseDate(dateStr string) (string, error) {
	t, err := time.Parse("02/01/2006", dateStr)
	if err != nil {
		return "", err
	}
	return t.Format("2006-01-02"), nil
}
