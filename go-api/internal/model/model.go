package model

type FundListItem struct {
	Code                  string  `json:"code"`
	Sector                string  `json:"sector"`
	PVP                   float64 `json:"p_vp"`
	DividendYield         float64 `json:"dividend_yield"`
	DividendYieldLast5Yrs float64 `json:"dividend_yield_last_5_years"`
	DailyLiquidity        float64 `json:"daily_liquidity"`
	NetWorth              float64 `json:"net_worth"`
	Type                  string  `json:"type"`
}

type FundListResponse struct {
	Total int            `json:"total"`
	Data  []FundListItem `json:"data"`
}

type FundDetails struct {
	ID                   string   `json:"id"`
	Code                 string   `json:"code"`
	RazaoSocial          string   `json:"razao_social"`
	CNPJ                 string   `json:"cnpj"`
	PublicoAlvo          string   `json:"publico_alvo"`
	Mandato              string   `json:"mandato"`
	Segmento             string   `json:"segmento"`
	TipoFundo            string   `json:"tipo_fundo"`
	PrazoDuracao         string   `json:"prazo_duracao"`
	TipoGestao           string   `json:"tipo_gestao"`
	TaxaAdminstracao     string   `json:"taxa_adminstracao"`
	DailyLiquidity       *float64 `json:"daily_liquidity"`
	Vacancia             float64  `json:"vacancia"`
	NumeroCotistas       float64  `json:"numero_cotistas"`
	CotasEmitidas        float64  `json:"cotas_emitidas"`
	ValorPatrimonialCota float64  `json:"valor_patrimonial_cota"`
	ValorPatrimonial     float64  `json:"valor_patrimonial"`
	UltimoRendimento     float64  `json:"ultimo_rendimento"`
}

type IndicatorItem struct {
	Year  string   `json:"year"`
	Value *float64 `json:"value"`
}

type NormalizedIndicators map[string][]IndicatorItem

type CotationItem struct {
	Date  string  `json:"date"`
	Price float64 `json:"price"`
}

type NormalizedCotations struct {
	Real  []CotationItem `json:"real"`
	Dolar []CotationItem `json:"dolar"`
	Euro  []CotationItem `json:"euro"`
}

type DividendType string

const (
	Dividendos  DividendType = "Dividendos"
	Amortizacao DividendType = "Amortização"
)

type DividendData struct {
	Value   float64      `json:"value"`
	Yield   float64      `json:"yield"`
	Date    string       `json:"date"`
	Payment string       `json:"payment"`
	Type    DividendType `json:"type"`
}

type CotationTodayItem struct {
	Price float64 `json:"price"`
	Hour  string  `json:"hour"`
}

type DocumentData struct {
	ID         int64  `json:"id"`
	Title      string `json:"title"`
	Category   string `json:"category"`
	Type       string `json:"type"`
	Date       string `json:"date"`
	DateUpload string `json:"dateUpload"`
	URL        string `json:"url"`
	Status     string `json:"status"`
	Version    int64  `json:"version"`
}

type TelegramUpdate struct {
	Message       *TelegramMessage       `json:"message"`
	CallbackQuery *TelegramCallbackQuery `json:"callback_query"`
}

type TelegramCallbackQuery struct {
	ID      string           `json:"id"`
	Data    string           `json:"data"`
	Message *TelegramMessage `json:"message"`
}

type TelegramMessage struct {
	MessageID int          `json:"message_id"`
	Text      string       `json:"text"`
	Chat      TelegramChat `json:"chat"`
}

type TelegramChat struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}
