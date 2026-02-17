package telegram

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func FormatFundsListMessage(funds []string) string {
	if len(funds) == 0 {
		return "📭 Sua lista está vazia."
	}
	return fmt.Sprintf("📌 Sua lista (%d fundos):\n%s", len(funds), strings.Join(uniqueUppercase(funds), ", "))
}

func FormatSetMessage(existing []string, added []string, removed []string, missing []string) string {
	lines := []string{}
	if len(existing) == 0 {
		lines = append(lines, "✅ Lista atualizada (vazia)")
	} else {
		lines = append(lines, fmt.Sprintf("✅ Lista atualizada (%d fundos)", len(existing)))
		lines = append(lines, "", "📌 Fundos", strings.Join(existing, ", "))
	}
	if len(added) > 0 {
		lines = append(lines, "", "➕ Adicionados", strings.Join(added, ", "))
	}
	if len(removed) > 0 {
		lines = append(lines, "", "➖ Removidos", strings.Join(removed, ", "))
	}
	if len(missing) > 0 {
		lines = append(lines, "", "❓ Não encontrei no banco", strings.Join(missing, ", "))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatAddMessage(addedCount int, nowList []string, missing []string) string {
	lines := []string{fmt.Sprintf("➕ Adicionados: %d", addedCount)}
	if len(nowList) == 0 {
		lines = append(lines, "📭 Agora: (vazia)")
	} else {
		lines = append(lines, fmt.Sprintf("📌 Agora (%d fundos)", len(nowList)), strings.Join(uniqueUppercase(nowList), ", "))
	}
	if len(missing) > 0 {
		lines = append(lines, "", "❓ Não encontrei no banco", strings.Join(uniqueUppercase(missing), ", "))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatRemoveMessage(removedCount int, nowList []string, missing []string) string {
	lines := []string{fmt.Sprintf("➖ Removidos: %d", removedCount)}
	if len(nowList) == 0 {
		lines = append(lines, "📭 Agora: (vazia)")
	} else {
		lines = append(lines, fmt.Sprintf("📌 Agora (%d fundos)", len(nowList)), strings.Join(uniqueUppercase(nowList), ", "))
	}
	if len(missing) > 0 {
		lines = append(lines, "", "❓ Não encontrei no banco", strings.Join(uniqueUppercase(missing), ", "))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatConfirmSetMessage(beforeCount int, afterCodes []string, added []string, removed []string, missing []string) string {
	lines := []string{
		"⚠️ Você está prestes a substituir sua lista.",
		fmt.Sprintf("Antes: %d", beforeCount),
		fmt.Sprintf("Depois: %d", len(afterCodes)),
	}
	if len(added) > 0 {
		lines = append(lines, "", "➕ Adicionados", strings.Join(added, ", "))
	}
	if len(removed) > 0 {
		lines = append(lines, "", "➖ Removidos", strings.Join(removed, ", "))
	}
	if len(missing) > 0 {
		lines = append(lines, "", "❓ Não encontrei no banco", strings.Join(missing, ", "))
	}
	lines = append(lines, "", "Confirmar?")
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatConfirmRemoveMessage(beforeCount int, toRemove []string, missing []string) string {
	lines := []string{
		"⚠️ Você está prestes a remover fundos da sua lista.",
		fmt.Sprintf("Lista atual: %d", beforeCount),
	}
	if len(toRemove) > 0 {
		lines = append(lines, "", "➖ A remover", strings.Join(uniqueUppercase(toRemove), ", "))
	}
	if len(missing) > 0 {
		lines = append(lines, "", "❓ Não encontrei no banco", strings.Join(uniqueUppercase(missing), ", "))
	}
	lines = append(lines, "", "Confirmar?")
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatDocumentsMessage(docs []LatestDocumentRow, limit int, code string) string {
	if len(docs) == 0 {
		if strings.TrimSpace(code) != "" {
			return "📰 Não encontrei documentos para " + strings.ToUpper(code) + "."
		}
		return "📰 Não encontrei documentos para sua lista."
	}

	header := "📰 Documentos — sua lista"
	if strings.TrimSpace(code) != "" {
		header = "📰 Documentos — " + strings.ToUpper(code)
	}
	sub := fmt.Sprintf("Mostrando %d de %d (mais recentes)", len(docs), limit)
	lines := []string{header, sub, ""}
	for _, d := range docs {
		fc := strings.ToUpper(strings.TrimSpace(d.FundCode))
		title := strings.TrimSpace(d.Title)
		date := strings.TrimSpace(d.DateUpload)
		docType := strings.TrimSpace(strings.Join(filterEmpty([]string{strings.TrimSpace(d.Category), strings.TrimSpace(d.Type)}), " · "))
		url := strings.TrimSpace(d.URL)

		line := "📌 " + fc
		if date != "" {
			line += " • " + date
		}
		lines = append(lines, line)
		if docType != "" {
			lines = append(lines, "🗂️ "+docType)
		}
		if title != "" {
			lines = append(lines, "📝 "+title)
		}
		if url != "" {
			lines = append(lines, "🔗 "+url)
		}
		lines = append(lines, "")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func filterEmpty(items []string) []string {
	out := make([]string, 0, len(items))
	for _, v := range items {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}

func CleanLine(value any) string {
	s := strings.TrimSpace(fmt.Sprint(value))
	if s == "" || s == "<nil>" {
		return ""
	}
	return strings.Join(strings.Fields(s), " ")
}

var dateISORe = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})`)
var dateBRFullRe = regexp.MustCompile(`^\d{2}/\d{2}/\d{4}$`)
var dateBRMonthYearRe = regexp.MustCompile(`^\d{2}/\d{4}$`)

func FormatDateHuman(value any) string {
	v := CleanLine(value)
	if v == "" {
		return ""
	}
	if m := dateISORe.FindStringSubmatch(v); len(m) == 4 {
		return fmt.Sprintf("%s/%s/%s", m[3], m[2], m[1])
	}
	if dateBRFullRe.MatchString(v) {
		return v
	}
	if dateBRMonthYearRe.MatchString(v) {
		return v
	}
	return v
}

func FormatNewDocumentMessage(fundCode string, d model.DocumentData) string {
	code := strings.ToUpper(CleanLine(fundCode))
	id := ""
	if d.ID > 0 {
		id = strconv.FormatInt(d.ID, 10)
	}
	title := CleanLine(d.Title)
	category := CleanLine(d.Category)
	typ := CleanLine(d.Type)
	status := CleanLine(d.Status)
	version := ""
	if d.Version > 0 {
		version = strconv.FormatInt(d.Version, 10)
	}
	url := CleanLine(d.URL)

	docType := strings.TrimSpace(strings.Join(filterEmpty([]string{category, typ}), " · "))

	upload := FormatDateHuman(d.DateUpload)
	ref := FormatDateHuman(d.Date)
	when := ""
	if upload != "" {
		when = fmt.Sprintf("🗓️ Upload: %s", upload)
		if ref != "" && ref != upload {
			when = when + fmt.Sprintf(" (ref: %s)", ref)
		}
	} else if ref != "" {
		when = fmt.Sprintf("🗓️ Ref: %s", ref)
	}

	header := fmt.Sprintf("📰 Novo documento — %s", code)
	lines := []string{header}
	if docType != "" {
		lines = append(lines, fmt.Sprintf("🗂️ %s", docType))
	}
	if title != "" {
		lines = append(lines, fmt.Sprintf("📝 %s", title))
	}
	if when != "" {
		lines = append(lines, when)
	}
	if status != "" {
		lines = append(lines, fmt.Sprintf("📌 Status: %s", status))
	}
	if version != "" && version != "1" {
		lines = append(lines, fmt.Sprintf("🔢 Versão: %s", version))
	}
	if id != "" {
		lines = append(lines, fmt.Sprintf("🆔 ID: %s", id))
	}
	if url != "" {
		lines = append(lines, fmt.Sprintf("🔗 %s", url))
	}
	lines = append(lines, fmt.Sprintf("📚 Ver mais: /documentos %s", code))
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

type RankHojeItem struct {
	Code                 string
	PVP                  float64
	DividendYieldMonthly float64
	Sharpe               float64
	TodayReturn          float64
}

type RankVItem struct {
	Code                 string
	PVP                  float64
	DividendYieldMonthly float64
	Regularity           float64
	TodayReturn          float64
}

func FormatRankHojeMessage(items []RankHojeItem, total int, missing []string) string {
	lines := []string{
		"🏆 Rank hoje — Value Investing FII (v2)",
		"Filtro: 0.35 <= P/VP <= 0.83 | DY mensal > 1,18% | Sharpe > 1.8",
		fmt.Sprintf("Selecionados: %d de %d%s", len(items), total, func() string {
			if len(missing) == 0 {
				return ""
			}
			return fmt.Sprintf(" (%d não encontrados)", len(missing))
		}()),
	}
	if len(items) == 0 {
		lines = append(lines, "", "Nenhum fundo atende aos critérios agora.")
		return strings.TrimSpace(strings.Join(lines, "\n"))
	}

	lines = append(lines, "", "Aporte Prioritário:")
	maxItems := 20
	shown := items
	if maxItems > 0 && len(items) > maxItems {
		shown = items[:maxItems]
	}
	for i, it := range shown {
		lines = append(lines, fmt.Sprintf(
			"%d. %s — Dia %s | P/VP %s | DY mensal %s | Sharpe %s",
			i+1,
			strings.ToUpper(strings.TrimSpace(it.Code)),
			formatSignedPctPtBR(it.TodayReturn, 2),
			formatNumberPtBR(it.PVP, 2),
			formatPctPtBR(it.DividendYieldMonthly, 2),
			formatNumberPtBR(it.Sharpe, 2),
		))
	}
	if len(shown) < len(items) {
		lines = append(lines, fmt.Sprintf("… +%d itens", len(items)-len(shown)))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatRankVMessage(items []RankVItem, total int) string {
	lines := []string{
		"🏆 RankV — Value (todos os fundos)",
		"Filtro: P/VP <= 0,70 | DY mensal > 1,16% | Pagou todos os meses",
		fmt.Sprintf("Selecionados: %d de %d", len(items), total),
	}
	if len(items) == 0 {
		lines = append(lines, "", "Nenhum fundo atende aos critérios agora.")
		return strings.TrimSpace(strings.Join(lines, "\n"))
	}

	lines = append(lines, "", "Aporte Prioritário:")
	maxItems := 20
	shown := items
	if maxItems > 0 && len(items) > maxItems {
		shown = items[:maxItems]
	}
	for i, it := range shown {
		lines = append(lines, fmt.Sprintf(
			"%d. %s — Dia %s | P/VP %s | DY mensal %s | Regularidade %s",
			i+1,
			strings.ToUpper(strings.TrimSpace(it.Code)),
			formatSignedPctPtBR(it.TodayReturn, 2),
			formatNumberPtBR(it.PVP, 2),
			formatPctPtBR(it.DividendYieldMonthly, 2),
			formatPctPtBR(it.Regularity, 1),
		))
	}
	if len(shown) < len(items) {
		lines = append(lines, fmt.Sprintf("… +%d itens", len(items)-len(shown)))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func formatNumberPtBR(v float64, decimals int) string {
	if decimals <= 0 {
		return strings.ReplaceAll(fmt.Sprintf("%.0f", v), ".", ",")
	}
	return strings.ReplaceAll(fmt.Sprintf("%."+strconv.Itoa(decimals)+"f", v), ".", ",")
}

func formatPctPtBR(v float64, decimals int) string {
	return fmt.Sprintf("%s%%", formatNumberPtBR(v*100, decimals))
}

func formatSignedPctPtBR(v float64, decimals int) string {
	p := v * 100
	if p > 0 {
		return "+" + formatNumberPtBR(p, decimals) + "%"
	}
	return formatNumberPtBR(p, decimals) + "%"
}

func clipText(value string, maxChars int) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	if maxChars <= 0 || len(v) <= maxChars {
		return v
	}
	if maxChars == 1 {
		return "…"
	}
	return strings.TrimSpace(v[:maxChars-1]) + "…"
}

func normalizeCategoryKey(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	r := strings.NewReplacer(
		"á", "a", "à", "a", "â", "a", "ã", "a",
		"é", "e", "ê", "e",
		"í", "i",
		"ó", "o", "ô", "o", "õ", "o",
		"ú", "u",
		"ç", "c",
	)
	v = r.Replace(v)
	return v
}

func pickCategoryEmoji(category string) string {
	key := normalizeCategoryKey(category)
	if strings.Contains(key, "titulo") || strings.Contains(key, "valores mobiliarios") {
		return "📄"
	}
	if strings.Contains(key, "fiagro") {
		return "🌾"
	}
	if strings.Contains(key, "hibrid") || strings.Contains(key, "misto") {
		return "🏢"
	}
	if strings.Contains(key, "infra") {
		return "⚙️"
	}
	if strings.Contains(key, "logistic") || strings.Contains(key, "industr") || strings.Contains(key, "galp") {
		return "🏭"
	}
	if strings.Contains(key, "shopping") || strings.Contains(key, "varejo") {
		return "🛍️"
	}
	if strings.Contains(key, "lajes") || strings.Contains(key, "corporativ") {
		return "🏙️"
	}
	if strings.Contains(key, "hospital") {
		return "🏥"
	}
	if strings.Contains(key, "agencia") && strings.Contains(key, "banc") {
		return "🏦"
	}
	if strings.Contains(key, "educa") {
		return "🎓"
	}
	if strings.Contains(key, "hote") {
		return "🏨"
	}
	if strings.Contains(key, "residenc") {
		return "🏘️"
	}
	if strings.Contains(key, "fundo de fundos") || key == "fof" {
		return "🧺"
	}
	if strings.Contains(key, "fip") || strings.Contains(key, "participacoes") {
		return "🤝"
	}
	if strings.Contains(key, "tijolo") {
		return "🧱"
	}
	if strings.Contains(key, "papel") {
		return "📄"
	}
	if strings.Contains(key, "desenvolvimento") {
		return "🏗️"
	}
	if strings.Contains(key, "outro") {
		return "🧩"
	}
	if strings.Contains(key, "sem categoria") || strings.Contains(key, "desconhecid") {
		return "❓"
	}
	return "📌"
}

func FormatCategoriesMessage(funds []string, info []FundCategoryInfo) string {
	if len(funds) == 0 {
		return "Sua lista está vazia."
	}

	byCode := map[string]string{}
	for _, r := range info {
		picked := strings.TrimSpace(firstNonEmpty(r.Segmento, r.Sector, r.TipoFundo, r.Type))
		if picked == "" {
			picked = "(sem categoria)"
		}
		byCode[strings.ToUpper(strings.TrimSpace(r.Code))] = picked
	}

	groups := map[string][]string{}
	for _, code := range uniqueUppercase(funds) {
		cat := byCode[code]
		if strings.TrimSpace(cat) == "" {
			cat = "(sem categoria)"
		}
		groups[cat] = append(groups[cat], code)
	}

	type group struct {
		Cat   string
		Codes []string
	}
	list := make([]group, 0, len(groups))
	for cat, codes := range groups {
		list = append(list, group{Cat: cat, Codes: codes})
	}
	sort.Slice(list, func(i, j int) bool {
		byCount := len(list[j].Codes) - len(list[i].Codes)
		if byCount != 0 {
			return byCount < 0
		}
		return list[i].Cat < list[j].Cat
	})

	lines := []string{}
	for _, g := range list {
		emoji := pickCategoryEmoji(g.Cat)
		lines = append(lines, fmt.Sprintf("%s %s (%d)", emoji, g.Cat, len(g.Codes)))
		shown := g.Codes
		suffix := ""
		if len(shown) > 50 {
			shown = shown[:50]
			suffix = ", …"
		}
		lines = append(lines, strings.Join(shown, ", ")+suffix, "")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func FormatPesquisaMessage(f FundPesquisaInfo) string {
	code := strings.ToUpper(strings.TrimSpace(f.Code))
	lines := []string{fmt.Sprintf("🔎 Pesquisa — %s", code)}

	if strings.TrimSpace(f.RazaoSocial) != "" {
		lines = append(lines, strings.TrimSpace(f.RazaoSocial))
	}
	if strings.TrimSpace(f.CNPJ) != "" {
		lines = append(lines, "🏷️ CNPJ: "+strings.TrimSpace(f.CNPJ))
	}

	line1 := []string{}
	if strings.TrimSpace(f.Sector) != "" {
		line1 = append(line1, "📚 Setor: "+strings.TrimSpace(f.Sector))
	}
	if strings.TrimSpace(f.Type) != "" {
		line1 = append(line1, "🏷️ Tipo: "+strings.TrimSpace(f.Type))
	}
	if len(line1) > 0 {
		lines = append(lines, "", strings.Join(line1, " | "))
	}

	line2 := []string{}
	if strings.TrimSpace(f.Segmento) != "" {
		line2 = append(line2, "🗂️ Segmento: "+strings.TrimSpace(f.Segmento))
	}
	if strings.TrimSpace(f.TipoFundo) != "" {
		line2 = append(line2, "🏢 Fundo: "+strings.TrimSpace(f.TipoFundo))
	}
	if len(line2) > 0 {
		lines = append(lines, strings.Join(line2, " | "))
	}

	line3 := []string{}
	if f.PVP != nil && isFinite(*f.PVP) {
		line3 = append(line3, "📈 P/VP: "+formatNumberPtBR(*f.PVP, 2))
	}
	if f.DividendYield != nil && isFinite(*f.DividendYield) {
		line3 = append(line3, "💸 DY: "+formatNumberPtBR(*f.DividendYield, 2))
	}
	if f.DividendYieldLast5Yrs != nil && isFinite(*f.DividendYieldLast5Yrs) {
		line3 = append(line3, "💸 DY 5a: "+formatNumberPtBR(*f.DividendYieldLast5Yrs, 2))
	}
	if len(line3) > 0 {
		lines = append(lines, strings.Join(line3, " | "))
	}

	line4 := []string{}
	if f.DailyLiquidity != nil && isFinite(*f.DailyLiquidity) && *f.DailyLiquidity > 0 {
		line4 = append(line4, "💧 Liquidez: "+formatNumberPtBR(*f.DailyLiquidity, 0))
	}
	if f.NetWorth != nil && isFinite(*f.NetWorth) && *f.NetWorth > 0 {
		line4 = append(line4, "🏦 PL: "+formatNumberPtBR(*f.NetWorth, 0))
	}
	if len(line4) > 0 {
		lines = append(lines, strings.Join(line4, " | "))
	}

	line5 := []string{}
	if f.Vacancia != nil && isFinite(*f.Vacancia) {
		line5 = append(line5, "🏚️ Vacância: "+formatPctPtBR(*f.Vacancia, 2))
	}
	if f.NumeroCotistas != nil && *f.NumeroCotistas > 0 {
		line5 = append(line5, fmt.Sprintf("👥 Cotistas: %d", *f.NumeroCotistas))
	}
	if len(line5) > 0 {
		lines = append(lines, strings.Join(line5, " | "))
	}

	line6 := []string{}
	if f.UltimoRendimento != nil && isFinite(*f.UltimoRendimento) && *f.UltimoRendimento > 0 {
		line6 = append(line6, "🧾 Últ. rend.: R$ "+formatNumberPtBR(*f.UltimoRendimento, 2))
	}
	if f.ValorPatrimonialCota != nil && isFinite(*f.ValorPatrimonialCota) && *f.ValorPatrimonialCota > 0 {
		line6 = append(line6, "📕 VP/Cota: R$ "+formatNumberPtBR(*f.ValorPatrimonialCota, 2))
	}
	if len(line6) > 0 {
		lines = append(lines, strings.Join(line6, " | "))
	}

	extra := []string{}
	if strings.TrimSpace(f.PublicoAlvo) != "" {
		extra = append(extra, "🎯 Público: "+strings.TrimSpace(f.PublicoAlvo))
	}
	if strings.TrimSpace(f.Mandato) != "" {
		extra = append(extra, "🧭 Mandato: "+strings.TrimSpace(f.Mandato))
	}
	if strings.TrimSpace(f.TipoGestao) != "" {
		extra = append(extra, "🧑‍💼 Gestão: "+strings.TrimSpace(f.TipoGestao))
	}
	if strings.TrimSpace(f.PrazoDuracao) != "" {
		extra = append(extra, "⏳ Prazo: "+strings.TrimSpace(f.PrazoDuracao))
	}
	if strings.TrimSpace(f.TaxaAdminstracao) != "" {
		extra = append(extra, "🧾 Taxa adm.: "+strings.TrimSpace(f.TaxaAdminstracao))
	}
	if len(extra) > 0 {
		lines = append(lines, "", strings.Join(extra, "\n"))
	}

	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func formatOptSignedPctPtBR(v *float64, decimals int) string {
	if v == nil || !isFinite(*v) {
		return "—"
	}
	return formatSignedPctPtBR(*v, decimals)
}

func formatOptPctPtBR(v *float64, decimals int) string {
	if v == nil || !isFinite(*v) {
		return "—"
	}
	return formatPctPtBR(*v, decimals)
}

func FormatCotationMessage(fundCode string, asOfDate string, lastPrice float64, ret7 *float64, ret30 *float64, ret90 *float64, maxDrawdown *float64, vol30 *float64, vol90 *float64) string {
	code := strings.ToUpper(strings.TrimSpace(fundCode))
	lines := []string{
		"📈 Cotação — " + code,
		"🗓️ Data base: " + FormatDateHuman(asOfDate),
		"💰 Último preço: R$ " + formatNumberPtBR(lastPrice, 2),
		"",
		"📊 Variações",
		"- 7d: " + formatOptSignedPctPtBR(ret7, 2),
		"- 30d: " + formatOptSignedPctPtBR(ret30, 2),
		"- 90d: " + formatOptSignedPctPtBR(ret90, 2),
		"",
		"📉 Drawdown máximo: " + formatOptPctPtBR(maxDrawdown, 2),
	}

	v30 := "—"
	if vol30 != nil && isFinite(*vol30) {
		v30 = formatPctPtBR(*vol30, 2)
	}
	v90 := "—"
	if vol90 != nil && isFinite(*vol90) {
		v90 = formatPctPtBR(*vol90, 2)
	}
	lines = append(lines, "🌪️ Volatilidade (anualizada): 30d "+v30+" | 90d "+v90)
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func FormatExportMessage(generatedAt string, exportedCodes []string, missingCodes []string) string {
	t := strings.TrimSpace(generatedAt)
	stamp := t
	if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
		stamp = parsed.Local().Format("02/01/2006 15:04:05")
	} else if parsed, err := time.Parse(time.RFC3339, t); err == nil {
		stamp = parsed.Local().Format("02/01/2006 15:04:05")
	}

	lines := []string{
		"📤 Exportação de FIIs",
		"📅 Gerado: " + stamp,
		fmt.Sprintf("📁 Fundos exportados: %d", len(exportedCodes)),
		fmt.Sprintf("❌ Não encontrados: %d", len(missingCodes)),
	}
	if len(exportedCodes) > 0 {
		lines = append(lines, "", strings.Join(exportedCodes, ", "))
	}
	if len(missingCodes) > 0 {
		lines = append(lines, "", "⚠️ Não encontrados: "+strings.Join(missingCodes, ", "))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
