package telegram

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type Processor struct {
	Repo   *Repo
	Client *Client
	FII    *fii.Service
}

func (p *Processor) ProcessUpdate(ctx context.Context, update *model.TelegramUpdate) error {
	if strings.TrimSpace(p.Client.Token) == "" {
		return nil
	}
	if update == nil {
		return nil
	}

	callback := update.CallbackQuery
	callbackData := strings.TrimSpace(func() string {
		if callback == nil {
			return ""
		}
		return callback.Data
	}())

	msg := update.Message
	if msg == nil && callback != nil {
		msg = callback.Message
	}
	if msg == nil {
		return nil
	}

	text := strings.TrimSpace(msg.Text)
	if text == "" && callbackData == "" {
		return nil
	}

	chatIDStr := strconv.FormatInt(msg.Chat.ID, 10)
	if chatIDStr == "" {
		return nil
	}

	if err := p.Repo.UpsertUser(ctx, chatIDStr, msg.Chat.Username, msg.Chat.FirstName, msg.Chat.LastName); err != nil {
		return err
	}

	if callback != nil && strings.TrimSpace(callback.ID) != "" {
		_ = p.Client.AckCallbackQuery(ctx, callback.ID)
	}

	cmd := botCommand{Kind: KindHelp}
	if kind, token, ok := ParseCallback(callbackData); ok {
		switch kind {
		case KindConfirm:
			cmd = botCommand{Kind: KindConfirm, Code: token}
		case KindCancel:
			cmd = botCommand{Kind: KindCancel, Code: token}
		}
	} else {
		cmd = ParseBotCommand(text)
	}

	switch cmd.Kind {
	case KindHelp:
		return p.handleHelp(ctx, chatIDStr)
	case KindList:
		return p.handleList(ctx, chatIDStr)
	case KindSet:
		return p.handleSet(ctx, chatIDStr, cmd.Codes)
	case KindAdd:
		return p.handleAdd(ctx, chatIDStr, cmd.Codes)
	case KindRemove:
		return p.handleRemove(ctx, chatIDStr, cmd.Codes)
	case KindDocumentos:
		return p.handleDocumentos(ctx, chatIDStr, cmd.Code, cmd.Limit)
	case KindRankHoje:
		return p.handleRankHoje(ctx, chatIDStr, cmd.Codes)
	case KindRankV:
		return p.handleRankV(ctx, chatIDStr, cmd.Codes)
	case KindCancel:
		return p.handleCancel(ctx, chatIDStr, cmd.Code)
	case KindConfirm:
		return p.handleConfirm(ctx, chatIDStr, cmd.Code)
	default:
		return p.handleHelp(ctx, chatIDStr)
	}
}

func (p *Processor) handleHelp(ctx context.Context, chatID string) error {
	text := strings.TrimSpace(strings.Join([]string{
		"Comandos:",
		"/lista — ver sua lista",
		"/set CODE1 CODE2 ... — substituir sua lista",
		"/add CODE1 CODE2 ... — adicionar fundos",
		"/remove CODE1 CODE2 ... — remover fundos",
		"/documentos [CODE] [LIMITE] — listar documentos recentes",
		"/rank hoje [CODE1 CODE2 ...] — rank para sua lista (ou codes)",
		"/rankv [CODE1 CODE2 ...] — rank value (ou todos os fundos)",
	}, "\n"))
	return p.Client.SendText(ctx, chatID, text, nil)
}

func (p *Processor) handleList(ctx context.Context, chatID string) error {
	funds, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatFundsListMessage(funds), nil)
}

func (p *Processor) handleSet(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /set CODE1 CODE2 ...", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)

	before, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}

	removed := diffStrings(before, existing)
	added := diffStrings(existing, before)

	if len(removed) > 0 {
		createdAt, err := p.Repo.UpsertPendingAction(ctx, chatID, PendingAction{Kind: PendingKindSet, Codes: uppercaseAll(codes)})
		if err != nil {
			return err
		}
		msg := FormatConfirmSetMessage(len(before), existing, added, removed, missing)
		rm := &ReplyMarkup{
			InlineKeyboard: [][]InlineKeyboardButton{{
				{Text: "✅ Confirmar", CallbackData: "confirm:" + createdAt},
				{Text: "❌ Cancelar", CallbackData: "cancel:" + createdAt},
			}},
		}
		return p.Client.SendText(ctx, chatID, msg, rm)
	}

	if err := p.Repo.SetUserFunds(ctx, chatID, existing); err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatSetMessage(existing, added, removed, missing), nil)
}

func (p *Processor) handleAdd(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /add CODE1 CODE2 ...", nil)
	}
	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)
	addedCount, err := p.Repo.AddUserFunds(ctx, chatID, existing)
	if err != nil {
		return err
	}
	nowList, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatAddMessage(addedCount, nowList, missing), nil)
}

func (p *Processor) handleRemove(ctx context.Context, chatID string, codes []string) error {
	if len(codes) == 0 {
		return p.Client.SendText(ctx, chatID, "Envie: /remove CODE1 CODE2 ...", nil)
	}
	existing, err := p.Repo.ListExistingFundCodes(ctx, codes)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(codes), existing)

	before, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}

	beforeSet := map[string]struct{}{}
	for _, c := range before {
		beforeSet[c] = struct{}{}
	}
	var toRemove []string
	for _, c := range existing {
		if _, ok := beforeSet[c]; ok {
			toRemove = append(toRemove, c)
		}
	}

	if len(toRemove) > 0 {
		createdAt, err := p.Repo.UpsertPendingAction(ctx, chatID, PendingAction{Kind: PendingKindRemove, Codes: uppercaseAll(codes)})
		if err != nil {
			return err
		}
		msg := FormatConfirmRemoveMessage(len(before), toRemove, missing)
		rm := &ReplyMarkup{
			InlineKeyboard: [][]InlineKeyboardButton{{
				{Text: "✅ Confirmar", CallbackData: "confirm:" + createdAt},
				{Text: "❌ Cancelar", CallbackData: "cancel:" + createdAt},
			}},
		}
		return p.Client.SendText(ctx, chatID, msg, rm)
	}

	return p.Client.SendText(ctx, chatID, FormatRemoveMessage(0, before, missing), nil)
}

func pickLimit(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 50 {
		return 50
	}
	return value
}

func (p *Processor) handleDocumentos(ctx context.Context, chatID string, code string, limit int) error {
	lim := pickLimit(limit, 5)
	fundCode := strings.TrimSpace(strings.ToUpper(code))
	if fundCode != "" {
		existing, err := p.Repo.ListExistingFundCodes(ctx, []string{fundCode})
		if err != nil {
			return err
		}
		if len(existing) == 0 {
			return p.Client.SendText(ctx, chatID, "Fundo não encontrado: "+fundCode, nil)
		}
		docs, err := p.Repo.ListLatestDocuments(ctx, []string{fundCode}, lim)
		if err != nil {
			return err
		}
		return p.Client.SendText(ctx, chatID, FormatDocumentsMessage(docs, lim, fundCode), nil)
	}

	funds, err := p.Repo.ListUserFunds(ctx, chatID)
	if err != nil {
		return err
	}
	if len(funds) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}
	docs, err := p.Repo.ListLatestDocuments(ctx, funds, lim)
	if err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, FormatDocumentsMessage(docs, lim, ""), nil)
}

func (p *Processor) handleRankHoje(ctx context.Context, chatID string, codes []string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

	requested := uniqueUppercase(codes)
	if len(requested) == 0 {
		funds, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		requested = uniqueUppercase(funds)
	}
	if len(requested) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, requested)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(requested), existing)

	ranked := make([]RankHojeItem, 0, len(existing))
	for _, code := range existing {
		exp, found, err := p.FII.ExportFund(ctx, code, fii.ExportFundOptions{})
		if err != nil || !found || exp == nil || exp.Fund == nil {
			continue
		}

		vacancia := exp.Fund.Vacancia
		dailyLiquidity := 0.0
		if exp.Fund.DailyLiquidity != nil && *exp.Fund.DailyLiquidity > 0 {
			dailyLiquidity = *exp.Fund.DailyLiquidity
		}

		pvp := exp.Metrics.Valuation.PVPCurrent
		dyMonthly := exp.Metrics.DividendYield.MonthlyMean
		sharpe := exp.Metrics.Risk.Sharpe
		todayReturn := exp.Metrics.Today.Return
		last3dReturn := exp.Metrics.Price.Last3dReturn

		notMelting := todayReturn > -0.02 && last3dReturn > -0.05
		if pvp < 0.94 && dyMonthly > 0.011 && vacancia == 0 && dailyLiquidity > 300_000 && sharpe >= 1.7 && notMelting {
			ranked = append(ranked, RankHojeItem{
				Code:                code,
				PVP:                 pvp,
				DividendYieldMonthly: dyMonthly,
				Sharpe:              sharpe,
				TodayReturn:         todayReturn,
			})
		}
	}

	sort.Slice(ranked, func(i, j int) bool {
		dy := ranked[j].DividendYieldMonthly - ranked[i].DividendYieldMonthly
		if dy != 0 {
			return dy < 0
		}
		sh := ranked[j].Sharpe - ranked[i].Sharpe
		if sh != 0 {
			return sh < 0
		}
		return ranked[i].PVP < ranked[j].PVP
	})

	return p.Client.SendText(ctx, chatID, FormatRankHojeMessage(ranked, len(existing), missing), nil)
}

func (p *Processor) handleRankV(ctx context.Context, chatID string, codes []string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

	_ = codes
	allCodes, err := p.Repo.ListAllFundCodes(ctx)
	if err != nil {
		return err
	}

	if len(allCodes) == 0 {
		return p.Client.SendText(ctx, chatID, "Não encontrei fundos na base.", nil)
	}

	ranked := make([]RankVItem, 0, len(allCodes))
	for _, code := range allCodes {
		exp, found, err := p.FII.ExportFund(ctx, code, fii.ExportFundOptions{})
		if err != nil || !found || exp == nil {
			continue
		}

		pvp := exp.Metrics.Valuation.PVPCurrent
		dyMonthly := exp.Metrics.DividendYield.MonthlyMean
		regularity := exp.Metrics.Dividends.Regularity
		monthsWithoutPayment := exp.Metrics.Dividends.MonthsWithoutPayment
		dividendCv := exp.Metrics.Dividends.CV
		dividendTrend := exp.Metrics.Dividends.TrendSlope
		drawdownMax := exp.Metrics.Risk.DrawdownMax
		recoveryDays := exp.Metrics.Risk.RecoveryTimeDays
		volAnnual := exp.Metrics.Risk.VolatilityAnnualized
		pvpPercentile := exp.Metrics.Valuation.PVPPercentile
		liqMean := exp.Metrics.Liquidity.Mean
		pctDaysTraded := exp.Metrics.Liquidity.PctDaysTraded
		last3dReturn := exp.Metrics.Price.Last3dReturn
		todayReturn := exp.Metrics.Today.Return

		series, ok := lastYearDividendSeries(exp.Data.Dividends)
		if !ok {
			continue
		}
		dividendValues := make([]float64, 0, len(series))
		for _, it := range series {
			dividendValues = append(dividendValues, it.Value)
		}
		dividendMax := maxFloat(dividendValues)
		dividendMin := minFloat(dividendValues)
		dividendMean := meanFloat(dividendValues)
		lastDividend := series[len(series)-1].Value

		prevMean := 0.0
		hasPrevMean := false
		if len(series) >= 4 {
			prevValues := dividendValues[:len(dividendValues)-1]
			prevMean = meanFloat(prevValues)
			hasPrevMean = prevMean > 0
		}

		split := 0
		if len(series) >= 6 {
			split = len(series) / 2
		}
		firstHalfMean := 0.0
		lastHalfMean := 0.0
		hasHalves := false
		if split >= 3 && len(series)-split >= 3 {
			firstHalfMean = meanFloat(dividendValues[:split])
			lastHalfMean = meanFloat(dividendValues[split:])
			hasHalves = firstHalfMean > 0 && lastHalfMean > 0
		}

		spikeOk := dividendMean > 0 && dividendMax <= dividendMean*2.5
		lastSpikeOk := !hasPrevMean || lastDividend <= prevMean*2.2
		minOk := dividendMean > 0 && dividendMin >= dividendMean*0.4
		regimeOk := !hasHalves || lastHalfMean <= firstHalfMean*1.8
		regularityYear := math.Min(1, float64(len(series))/12.0)
		notMelting := todayReturn > -0.01 && last3dReturn >= 0

		if pvp <= 0.7 &&
			dyMonthly > 0.0116 &&
			monthsWithoutPayment == 0 &&
			regularityYear >= 0.999 &&
			dividendCv <= 0.6 &&
			dividendTrend > 0 &&
			drawdownMax > -0.25 &&
			recoveryDays <= 120 &&
			volAnnual <= 0.3 &&
			pvpPercentile <= 0.25 &&
			liqMean >= 400000 &&
			pctDaysTraded >= 0.95 &&
			spikeOk &&
			lastSpikeOk &&
			minOk &&
			regimeOk &&
			notMelting {
			ranked = append(ranked, RankVItem{
				Code:                code,
				PVP:                 pvp,
				DividendYieldMonthly: dyMonthly,
				Regularity:          regularity,
				TodayReturn:         todayReturn,
			})
		}
	}

	sort.Slice(ranked, func(i, j int) bool {
		dy := ranked[j].DividendYieldMonthly - ranked[i].DividendYieldMonthly
		if dy != 0 {
			return dy < 0
		}
		pvp := ranked[i].PVP - ranked[j].PVP
		if pvp != 0 {
			return pvp < 0
		}
		return ranked[j].Regularity > ranked[i].Regularity
	})

	return p.Client.SendText(ctx, chatID, FormatRankVMessage(ranked, len(allCodes)), nil)
}

type dividendPoint struct {
	Iso   string
	Value float64
}

func lastYearDividendSeries(dividends []model.DividendData) ([]dividendPoint, bool) {
	if len(dividends) == 0 {
		return nil, false
	}
	cutoff := time.Now().UTC().AddDate(-1, 0, 0).Format("2006-01-02")
	out := make([]dividendPoint, 0, len(dividends))
	for _, d := range dividends {
		if d.Type != model.Dividendos || d.Value <= 0 {
			continue
		}
		iso := toDateIsoFromBr(d.Date)
		if iso == "" || iso < cutoff {
			continue
		}
		out = append(out, dividendPoint{Iso: iso, Value: d.Value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Iso < out[j].Iso })
	if len(out) < 12 {
		return nil, false
	}
	return out, true
}

func toDateIsoFromBr(dateBr string) string {
	parts := strings.Split(strings.TrimSpace(dateBr), "/")
	if len(parts) != 3 {
		return ""
	}
	dd := strings.TrimSpace(parts[0])
	mm := strings.TrimSpace(parts[1])
	yy := strings.TrimSpace(parts[2])
	if len(dd) != 2 || len(mm) != 2 || len(yy) != 4 {
		return ""
	}
	return yy + "-" + mm + "-" + dd
}

func meanFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	acc := 0.0
	for _, v := range values {
		acc += v
	}
	return acc / float64(len(values))
}

func maxFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func minFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func (p *Processor) handleCancel(ctx context.Context, chatID string, callbackToken string) error {
	pending, err := p.Repo.GetPendingAction(ctx, chatID)
	if err != nil {
		return err
	}
	if pending == nil {
		return p.Client.SendText(ctx, chatID, "Não há nenhuma ação pendente para cancelar.", nil)
	}
	if callbackToken != "" && pending.CreatedAt.Format(time.RFC3339Nano) != callbackToken && pending.CreatedAt.Format(time.RFC3339) != callbackToken {
		return p.Client.SendText(ctx, chatID, "Esse cancelamento não é mais válido. Envie o comando novamente.", nil)
	}
	if time.Since(pending.CreatedAt) > 10*time.Minute {
		_ = p.Repo.ClearPendingAction(ctx, chatID)
		return p.Client.SendText(ctx, chatID, "Esse cancelamento expirou. Envie o comando novamente.", nil)
	}
	if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
		return err
	}
	return p.Client.SendText(ctx, chatID, "❌ Cancelado.", nil)
}

func (p *Processor) handleConfirm(ctx context.Context, chatID string, callbackToken string) error {
	pending, err := p.Repo.GetPendingAction(ctx, chatID)
	if err != nil {
		return err
	}
	if pending == nil {
		return p.Client.SendText(ctx, chatID, "Não há nenhuma ação pendente para confirmar.", nil)
	}
	if callbackToken != "" && pending.CreatedAt.Format(time.RFC3339Nano) != callbackToken && pending.CreatedAt.Format(time.RFC3339) != callbackToken {
		return p.Client.SendText(ctx, chatID, "Essa confirmação não é mais válida. Envie o comando novamente.", nil)
	}
	if time.Since(pending.CreatedAt) > 10*time.Minute {
		_ = p.Repo.ClearPendingAction(ctx, chatID)
		return p.Client.SendText(ctx, chatID, "Essa confirmação expirou. Envie o comando novamente.", nil)
	}

	switch pending.Action.Kind {
	case PendingKindSet:
		existing, err := p.Repo.ListExistingFundCodes(ctx, pending.Action.Codes)
		if err != nil {
			return err
		}
		missing := diffStrings(uppercaseAll(pending.Action.Codes), existing)
		before, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		if err := p.Repo.SetUserFunds(ctx, chatID, existing); err != nil {
			return err
		}
		if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
			return err
		}
		removed := diffStrings(before, existing)
		added := diffStrings(existing, before)
		return p.Client.SendText(ctx, chatID, "✅ Confirmado\n\n"+FormatSetMessage(existing, added, removed, missing), nil)
	case PendingKindRemove:
		existing, err := p.Repo.ListExistingFundCodes(ctx, pending.Action.Codes)
		if err != nil {
			return err
		}
		missing := diffStrings(uppercaseAll(pending.Action.Codes), existing)
		removedCount, err := p.Repo.RemoveUserFunds(ctx, chatID, existing)
		if err != nil {
			return err
		}
		if err := p.Repo.ClearPendingAction(ctx, chatID); err != nil {
			return err
		}
		nowList, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		return p.Client.SendText(ctx, chatID, "✅ Confirmado\n\n"+FormatRemoveMessage(removedCount, nowList, missing), nil)
	}

	_ = p.Repo.ClearPendingAction(ctx, chatID)
	return p.Client.SendText(ctx, chatID, "Não consegui interpretar a ação pendente. Envie o comando novamente.", nil)
}
