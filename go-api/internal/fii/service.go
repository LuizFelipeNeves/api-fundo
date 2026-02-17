package fii

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type Service struct {
	DB *db.DB
}

func New(db *db.DB) *Service {
	return &Service{DB: db}
}

var fiiCodeRe = regexp.MustCompile(`^[A-Za-z]{4}11$`)
var isoDateRe = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})`)

func ValidateFundCode(raw string) (string, bool) {
	v := strings.TrimSpace(raw)
	if v == "" || !fiiCodeRe.MatchString(v) {
		return "", false
	}
	return strings.ToUpper(v), true
}

func toDateBrFromIso(dateIso string) string {
	m := isoDateRe.FindStringSubmatch(strings.TrimSpace(dateIso))
	if len(m) != 4 {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", m[3], m[2], m[1])
}

func (s *Service) ListFunds(ctx context.Context) (model.FundListResponse, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type
		FROM fund_master
		ORDER BY code ASC
	`)
	if err != nil {
		return model.FundListResponse{}, err
	}
	defer rows.Close()

	out := model.FundListResponse{}
	for rows.Next() {
		var (
			code                  string
			sector, typ           sql.NullString
			pvp, dy, dy5, liq, nw sql.NullFloat64
		)
		if err := rows.Scan(&code, &sector, &pvp, &dy, &dy5, &liq, &nw, &typ); err != nil {
			return model.FundListResponse{}, err
		}
		out.Data = append(out.Data, model.FundListItem{
			Code:                  strings.ToUpper(strings.TrimSpace(code)),
			Sector:                nullString(sector),
			PVP:                   nullFloat(pvp),
			DividendYield:         nullFloat(dy),
			DividendYieldLast5Yrs: nullFloat(dy5),
			DailyLiquidity:        nullFloat(liq),
			NetWorth:              nullFloat(nw),
			Type:                  nullString(typ),
		})
	}
	out.Total = len(out.Data)
	return out, rows.Err()
}

func nullString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func nullFloat(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

func (s *Service) GetFundDetails(ctx context.Context, code string) (*model.FundDetails, error) {
	var (
		id                                                                          sql.NullString
		rowCode                                                                     string
		razao, cnpj, publico, mandato, segmento, tipoFundo, prazo, tipoGestao, taxa sql.NullString
		daily                                                                       sql.NullFloat64
		vac, cotistas, emitidas, vpc, vp, ultimo                                    sql.NullFloat64
	)

	err := s.DB.QueryRowContext(ctx, `
		SELECT id, code, razao_social, cnpj, publico_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
		       taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
		       valor_patrimonial, ultimo_rendimento
		FROM fund_master
		WHERE code = $1
		LIMIT 1
	`, code).Scan(
		&id, &rowCode, &razao, &cnpj, &publico, &mandato, &segmento, &tipoFundo, &prazo, &tipoGestao,
		&taxa, &daily, &vac, &cotistas, &emitidas, &vpc, &vp, &ultimo,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !id.Valid || strings.TrimSpace(id.String) == "" || !cnpj.Valid || strings.TrimSpace(cnpj.String) == "" {
		return nil, nil
	}

	var dailyPtr *float64
	if daily.Valid {
		v := daily.Float64
		dailyPtr = &v
	}

	var vacPtr *float64
	if vac.Valid {
		v := vac.Float64
		vacPtr = &v
	}

	return &model.FundDetails{
		ID:                   id.String,
		Code:                 strings.ToUpper(strings.TrimSpace(rowCode)),
		RazaoSocial:          nullString(razao),
		CNPJ:                 nullString(cnpj),
		PublicoAlvo:          nullString(publico),
		Mandato:              nullString(mandato),
		Segmento:             nullString(segmento),
		TipoFundo:            nullString(tipoFundo),
		PrazoDuracao:         nullString(prazo),
		TipoGestao:           nullString(tipoGestao),
		TaxaAdminstracao:     nullString(taxa),
		DailyLiquidity:       dailyPtr,
		Vacancia:             vacPtr,
		NumeroCotistas:       nullFloat(cotistas),
		CotasEmitidas:        nullFloat(emitidas),
		ValorPatrimonialCota: nullFloat(vpc),
		ValorPatrimonial:     nullFloat(vp),
		UltimoRendimento:     nullFloat(ultimo),
	}, nil
}

func (s *Service) GetLatestIndicators(ctx context.Context, code string) (model.NormalizedIndicators, bool, error) {
	var raw []byte
	err := s.DB.QueryRowContext(ctx, `
		SELECT data_json
		FROM indicators_snapshot
		WHERE fund_code = $1
		ORDER BY fetched_at DESC
		LIMIT 1
	`, code).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var parsed model.NormalizedIndicators
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, false, err
	}
	return parsed, true, nil
}

func (s *Service) GetCotations(ctx context.Context, code string, days int) (*model.NormalizedCotations, error) {
	limit := 1825
	if days > 0 {
		if days > 5000 {
			days = 5000
		}
		limit = days
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT date_iso, price
		FROM cotation
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT $2
	`, code, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		dateIso string
		price   float64
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.dateIso, &r.price); err != nil {
			return nil, err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, nil
	}

	real := make([]model.CotationItem, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		real = append(real, model.CotationItem{
			Date:  toDateBrFromIso(all[i].dateIso),
			Price: all[i].price,
		})
	}

	return &model.NormalizedCotations{Real: real, Dolar: []model.CotationItem{}, Euro: []model.CotationItem{}}, nil
}

func dividendTypeFromCode(code int) (model.DividendType, bool) {
	if code == 1 {
		return model.Dividendos, true
	}
	if code == 2 {
		return model.Amortizacao, true
	}
	return "", false
}

func (s *Service) GetDividends(ctx context.Context, code string) ([]model.DividendData, bool, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT date_iso, payment, type, value, yield
		FROM dividend
		WHERE fund_code = $1
		ORDER BY date_iso DESC
	`, code)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []model.DividendData
	for rows.Next() {
		var (
			dateIso    string
			paymentIso string
			typeCode   int
			value      float64
			yield      float64
		)
		if err := rows.Scan(&dateIso, &paymentIso, &typeCode, &value, &yield); err != nil {
			return nil, false, err
		}
		t, ok := dividendTypeFromCode(typeCode)
		if !ok {
			continue
		}
		out = append(out, model.DividendData{
			Value:   value,
			Yield:   yield,
			Date:    toDateBrFromIso(dateIso),
			Payment: toDateBrFromIso(paymentIso),
			Type:    t,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func (s *Service) GetLatestCotationsToday(ctx context.Context, code string) ([]model.CotationTodayItem, bool, error) {
	var latestDate time.Time
	err := s.DB.QueryRowContext(ctx, `
		SELECT date_iso
		FROM cotation_today
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT 1
	`, code).Scan(&latestDate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT hour, price
		FROM cotation_today
		WHERE fund_code = $1 AND date_iso = $2
		ORDER BY hour ASC
	`, code, latestDate.Format("2006-01-02"))
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	out := []model.CotationTodayItem{}
	for rows.Next() {
		var it model.CotationTodayItem
		if err := rows.Scan(&it.Hour, &it.Price); err != nil {
			return nil, false, err
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	if len(out) == 0 {
		return nil, false, nil
	}

	return out, true, nil
}

func (s *Service) GetDocuments(ctx context.Context, code string) ([]model.DocumentData, bool, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT document_id, title, category, type, date, "dateUpload", url, status, version
		FROM document
		WHERE fund_code = $1
		ORDER BY date_upload_iso DESC, document_id DESC
	`, code)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []model.DocumentData
	for rows.Next() {
		var d model.DocumentData
		if err := rows.Scan(&d.ID, &d.Title, &d.Category, &d.Type, &d.Date, &d.DateUpload, &d.URL, &d.Status, &d.Version); err != nil {
			return nil, false, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}
