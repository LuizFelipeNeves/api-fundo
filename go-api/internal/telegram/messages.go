package telegram

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

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
	for i, it := range items {
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
	for i, it := range items {
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
