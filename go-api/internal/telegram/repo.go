package telegram

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/db"
)

type Repo struct {
	DB *db.DB
}

func NewRepo(db *db.DB) *Repo {
	return &Repo{DB: db}
}

type PendingAction struct {
	Kind  PendingKind `json:"kind"`
	Codes []string    `json:"codes"`
}

type PendingKind string

const (
	PendingKindSet    PendingKind = "set"
	PendingKindRemove PendingKind = "remove"
)

type PendingActionRow struct {
	CreatedAt time.Time
	Action    PendingAction
}

type LatestDocumentRow struct {
	FundCode   string
	Title      string
	Category   string
	Type       string
	DateUpload string
	URL        string
}

type LatestDocumentBrief struct {
	FundCode   string
	DocumentID int
	Category   string
	Type       string
	DateUpload string
	URL        string
}

type FundCategoryInfo struct {
	Code      string
	Segmento  string
	Sector    string
	TipoFundo string
	Type      string
}

type FundPesquisaInfo struct {
	Code                   string
	Sector                 string
	Type                   string
	Segmento               string
	TipoFundo              string
	PVP                    *float64
	DividendYield          *float64
	DividendYieldLast5Yrs  *float64
	DailyLiquidity         *float64
	NetWorth               *float64
	RazaoSocial            string
	CNPJ                   string
	PublicoAlvo            string
	Mandato                string
	PrazoDuracao           string
	TipoGestao             string
	TaxaAdminstracao       string
	Vacancia               *float64
	NumeroCotistas         *int64
	CotasEmitidas          *int64
	ValorPatrimonialCota   *float64
	ValorPatrimonial       *float64
	UltimoRendimento       *float64
	FundMasterUpdatedAtUTC *time.Time
}

func (r *Repo) UpsertUser(ctx context.Context, chatID string, username string, firstName string, lastName string) error {
	now := time.Now()
	_, err := r.DB.ExecContext(ctx, `
		INSERT INTO telegram_user (chat_id, username, first_name, last_name, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (chat_id) DO UPDATE SET
			username = EXCLUDED.username,
			first_name = EXCLUDED.first_name,
			last_name = EXCLUDED.last_name,
			updated_at = EXCLUDED.updated_at
	`, chatID, nullIfEmpty(username), nullIfEmpty(firstName), nullIfEmpty(lastName), now)
	return err
}

func nullIfEmpty(v string) any {
	s := strings.TrimSpace(v)
	if s == "" {
		return nil
	}
	return s
}

func (r *Repo) ListUserFunds(ctx context.Context, chatID string) ([]string, error) {
	rows, err := r.DB.QueryContext(ctx, `
		SELECT fund_code
		FROM telegram_user_fund
		WHERE chat_id = $1
		ORDER BY fund_code ASC
	`, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		out = append(out, strings.ToUpper(strings.TrimSpace(code)))
	}
	return out, rows.Err()
}

func (r *Repo) ListExistingFundCodes(ctx context.Context, codes []string) ([]string, error) {
	uniq := uniqueUppercase(codes)
	if len(uniq) == 0 {
		return []string{}, nil
	}

	rows, err := r.DB.QueryContext(ctx, `
		SELECT code
		FROM fund_master
		WHERE code = ANY($1)
		ORDER BY code ASC
	`, pq.Array(uniq))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		out = append(out, strings.ToUpper(strings.TrimSpace(code)))
	}
	return out, rows.Err()
}

func (r *Repo) ListAllFundCodes(ctx context.Context) ([]string, error) {
	rows, err := r.DB.QueryContext(ctx, `
		SELECT code
		FROM fund_master
		ORDER BY code ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		out = append(out, strings.ToUpper(strings.TrimSpace(code)))
	}
	return out, rows.Err()
}

func (r *Repo) SetUserFunds(ctx context.Context, chatID string, codes []string) error {
	uniq := uniqueUppercase(codes)
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM telegram_user_fund WHERE chat_id = $1`, chatID); err != nil {
		return err
	}

	now := time.Now()
	for _, code := range uniq {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO telegram_user_fund (chat_id, fund_code, created_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (chat_id, fund_code) DO NOTHING
		`, chatID, code, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *Repo) AddUserFunds(ctx context.Context, chatID string, codes []string) (int, error) {
	uniq := uniqueUppercase(codes)
	if len(uniq) == 0 {
		return 0, nil
	}
	now := time.Now()
	added := 0
	for _, code := range uniq {
		res, err := r.DB.ExecContext(ctx, `
			INSERT INTO telegram_user_fund (chat_id, fund_code, created_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (chat_id, fund_code) DO NOTHING
		`, chatID, code, now)
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			added += int(n)
		}
	}
	return added, nil
}

func (r *Repo) RemoveUserFunds(ctx context.Context, chatID string, codes []string) (int, error) {
	uniq := uniqueUppercase(codes)
	if len(uniq) == 0 {
		return 0, nil
	}
	res, err := r.DB.ExecContext(ctx, `
		DELETE FROM telegram_user_fund
		WHERE chat_id = $1 AND fund_code = ANY($2)
	`, chatID, pq.Array(uniq))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (r *Repo) UpsertPendingAction(ctx context.Context, chatID string, action PendingAction) (string, error) {
	now := time.Now()
	b, err := json.Marshal(action)
	if err != nil {
		return "", err
	}
	if _, err := r.DB.ExecContext(ctx, `
		INSERT INTO telegram_pending_action (chat_id, created_at, action_json)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (chat_id) DO UPDATE SET
			created_at = EXCLUDED.created_at,
			action_json = EXCLUDED.action_json
	`, chatID, now, string(b)); err != nil {
		return "", err
	}
	return now.UTC().Format(time.RFC3339Nano), nil
}

func (r *Repo) GetPendingAction(ctx context.Context, chatID string) (*PendingActionRow, error) {
	var createdAt time.Time
	var raw []byte
	err := r.DB.QueryRowContext(ctx, `
		SELECT created_at, action_json
		FROM telegram_pending_action
		WHERE chat_id = $1
	`, chatID).Scan(&createdAt, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var act PendingAction
	if err := json.Unmarshal(raw, &act); err != nil {
		return nil, err
	}
	return &PendingActionRow{CreatedAt: createdAt, Action: act}, nil
}

func (r *Repo) ClearPendingAction(ctx context.Context, chatID string) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM telegram_pending_action WHERE chat_id = $1`, chatID)
	return err
}

func (r *Repo) ListLatestDocuments(ctx context.Context, fundCodes []string, limit int) ([]LatestDocumentRow, error) {
	codes := uniqueUppercase(fundCodes)
	if len(codes) == 0 {
		return []LatestDocumentRow{}, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT
			fund_code,
			title,
			category,
			type,
			to_char("dateUpload" AT TIME ZONE 'UTC', 'DD/MM/YYYY') as "dateUpload",
			url
		FROM document
		WHERE fund_code = ANY($1)
		ORDER BY document."dateUpload" DESC, document_id DESC
		LIMIT $2
	`, pq.Array(codes), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LatestDocumentRow
	for rows.Next() {
		var rr LatestDocumentRow
		if err := rows.Scan(&rr.FundCode, &rr.Title, &rr.Category, &rr.Type, &rr.DateUpload, &rr.URL); err != nil {
			return nil, err
		}
		out = append(out, rr)
	}
	return out, rows.Err()
}

func (r *Repo) ListLatestDocumentsByFund(ctx context.Context, fundCodes []string) (map[string]LatestDocumentBrief, error) {
	codes := uniqueUppercase(fundCodes)
	if len(codes) == 0 {
		return map[string]LatestDocumentBrief{}, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT
			fund_code,
			document_id,
			category,
			type,
			to_char("dateUpload" AT TIME ZONE 'UTC', 'DD/MM/YYYY') as "dateUpload",
			url
		FROM document
		WHERE fund_code = ANY($1)
		ORDER BY document."dateUpload" DESC, document_id DESC
	`, pq.Array(codes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]LatestDocumentBrief{}
	for rows.Next() {
		var rr LatestDocumentBrief
		if err := rows.Scan(&rr.FundCode, &rr.DocumentID, &rr.Category, &rr.Type, &rr.DateUpload, &rr.URL); err != nil {
			return nil, err
		}
		code := strings.ToUpper(strings.TrimSpace(rr.FundCode))
		if code == "" {
			continue
		}
		if _, ok := out[code]; ok {
			continue
		}
		rr.FundCode = code
		out[code] = rr
	}
	return out, rows.Err()
}

func (r *Repo) ListFundCategoryInfoByCodes(ctx context.Context, fundCodes []string) ([]FundCategoryInfo, error) {
	codes := uniqueUppercase(fundCodes)
	if len(codes) == 0 {
		return []FundCategoryInfo{}, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT code, segmento, sector, tipo_fundo, type
		FROM fund_master
		WHERE code = ANY($1)
		ORDER BY code ASC
	`, pq.Array(codes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FundCategoryInfo
	for rows.Next() {
		var (
			code, segmento, sector, tipoFundo, typ sql.NullString
		)
		if err := rows.Scan(&code, &segmento, &sector, &tipoFundo, &typ); err != nil {
			return nil, err
		}
		out = append(out, FundCategoryInfo{
			Code:      strings.ToUpper(strings.TrimSpace(code.String)),
			Segmento:  strings.TrimSpace(segmento.String),
			Sector:    strings.TrimSpace(sector.String),
			TipoFundo: strings.TrimSpace(tipoFundo.String),
			Type:      strings.TrimSpace(typ.String),
		})
	}
	return out, rows.Err()
}

func (r *Repo) GetFundPesquisaInfo(ctx context.Context, fundCode string) (*FundPesquisaInfo, error) {
	code := strings.ToUpper(strings.TrimSpace(fundCode))
	if code == "" {
		return nil, nil
	}

	var (
		sector, typ, segmento, tipoFundo                          sql.NullString
		pvp, dy, dy5, dailyLiq, netWorth                          sql.NullFloat64
		razao, cnpj, publico, mandato, prazo, tipoGestao, taxaAdm sql.NullString
		vac, vpc, vp, ultimo                                      sql.NullFloat64
		numCotistas, cotasEmitidas                                sql.NullInt64
		updatedAt                                                 sql.NullTime
	)
	err := r.DB.QueryRowContext(ctx, `
		SELECT code,
		       sector,
		       type,
		       segmento,
		       tipo_fundo,
		       p_vp,
		       dividend_yield,
		       dividend_yield_last_5_years,
		       daily_liquidity,
		       net_worth,
		       razao_social,
		       cnpj,
		       publico_alvo,
		       mandato,
		       prazo_duracao,
		       tipo_gestao,
		       taxa_adminstracao,
		       vacancia,
		       numero_cotistas,
		       cotas_emitidas,
		       valor_patrimonial_cota,
		       valor_patrimonial,
		       ultimo_rendimento,
		       updated_at
		FROM fund_master
		WHERE code = $1
		LIMIT 1
	`, code).Scan(
		&code,
		&sector,
		&typ,
		&segmento,
		&tipoFundo,
		&pvp,
		&dy,
		&dy5,
		&dailyLiq,
		&netWorth,
		&razao,
		&cnpj,
		&publico,
		&mandato,
		&prazo,
		&tipoGestao,
		&taxaAdm,
		&vac,
		&numCotistas,
		&cotasEmitidas,
		&vpc,
		&vp,
		&ultimo,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	floatPtr := func(v sql.NullFloat64) *float64 {
		if !v.Valid {
			return nil
		}
		x := v.Float64
		return &x
	}
	int64Ptr := func(v sql.NullInt64) *int64 {
		if !v.Valid {
			return nil
		}
		x := v.Int64
		return &x
	}
	timePtr := func(v sql.NullTime) *time.Time {
		if !v.Valid {
			return nil
		}
		t := v.Time.UTC()
		return &t
	}

	return &FundPesquisaInfo{
		Code:                   strings.ToUpper(strings.TrimSpace(code)),
		Sector:                 strings.TrimSpace(sector.String),
		Type:                   strings.TrimSpace(typ.String),
		Segmento:               strings.TrimSpace(segmento.String),
		TipoFundo:              strings.TrimSpace(tipoFundo.String),
		PVP:                    floatPtr(pvp),
		DividendYield:          floatPtr(dy),
		DividendYieldLast5Yrs:  floatPtr(dy5),
		DailyLiquidity:         floatPtr(dailyLiq),
		NetWorth:               floatPtr(netWorth),
		RazaoSocial:            strings.TrimSpace(razao.String),
		CNPJ:                   strings.TrimSpace(cnpj.String),
		PublicoAlvo:            strings.TrimSpace(publico.String),
		Mandato:                strings.TrimSpace(mandato.String),
		PrazoDuracao:           strings.TrimSpace(prazo.String),
		TipoGestao:             strings.TrimSpace(tipoGestao.String),
		TaxaAdminstracao:       strings.TrimSpace(taxaAdm.String),
		Vacancia:               floatPtr(vac),
		NumeroCotistas:         int64Ptr(numCotistas),
		CotasEmitidas:          int64Ptr(cotasEmitidas),
		ValorPatrimonialCota:   floatPtr(vpc),
		ValorPatrimonial:       floatPtr(vp),
		UltimoRendimento:       floatPtr(ultimo),
		FundMasterUpdatedAtUTC: timePtr(updatedAt),
	}, nil
}

func uniqueUppercase(items []string) []string {
	set := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, v := range items {
		s := strings.ToUpper(strings.TrimSpace(v))
		if s == "" {
			continue
		}
		if _, ok := set[s]; ok {
			continue
		}
		set[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func uppercaseAll(items []string) []string {
	out := make([]string, 0, len(items))
	for _, v := range items {
		s := strings.ToUpper(strings.TrimSpace(v))
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func diffStrings(a []string, b []string) []string {
	bset := map[string]struct{}{}
	for _, v := range b {
		bset[strings.ToUpper(strings.TrimSpace(v))] = struct{}{}
	}
	var out []string
	for _, v := range a {
		u := strings.ToUpper(strings.TrimSpace(v))
		if u == "" {
			continue
		}
		if _, ok := bset[u]; ok {
			continue
		}
		out = append(out, u)
	}
	return uniqueUppercase(out)
}
