package telegram

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
)

type botCommand struct {
	Kind  CommandKind
	Codes []string
	Code  string
	Limit int
}

type CommandKind string

const (
	KindHelp       CommandKind = "help"
	KindList       CommandKind = "list"
	KindCategories CommandKind = "categories"
	KindExport     CommandKind = "export"
	KindResumoDoc  CommandKind = "resumo-documento"
	KindSet        CommandKind = "set"
	KindAdd        CommandKind = "add"
	KindRemove     CommandKind = "remove"
	KindDocumentos CommandKind = "documentos"
	KindPesquisa   CommandKind = "pesquisa"
	KindCotation   CommandKind = "cotation"
	KindRankHoje   CommandKind = "rank_hoje"
	KindRankV      CommandKind = "rankv"
	KindCancel     CommandKind = "cancel"
	KindConfirm    CommandKind = "confirm"
)

var codeInTextRe = regexp.MustCompile(`(?i)\b[a-z]{4}11\b`)
var callbackRe = regexp.MustCompile(`^(confirm|cancel)(?::(.+))?$`)

func ParseBotCommand(text string) botCommand {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return botCommand{Kind: KindHelp}
	}
	if !strings.HasPrefix(raw, "/") {
		return botCommand{Kind: KindHelp}
	}

	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return botCommand{Kind: KindHelp}
	}

	head := strings.ToLower(strings.TrimSpace(parts[0]))
	if i := strings.Index(head, "@"); i >= 0 {
		head = head[:i]
	}

	tail := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
	switch head {
	case "/start", "/help", "/menu", "/ajuda":
		return botCommand{Kind: KindHelp}
	case "/lista", "/list":
		return botCommand{Kind: KindList}
	case "/categorias", "/categoria", "/categories":
		return botCommand{Kind: KindCategories}
	case "/export", "/exportar":
		return botCommand{Kind: KindExport, Codes: extractFundCodes(tail)}
	case "/resumo-documento", "/resumo_documento", "/resumodocumento":
		return botCommand{Kind: KindResumoDoc, Codes: extractFundCodes(tail)}
	case "/set":
		return botCommand{Kind: KindSet, Codes: extractFundCodes(tail)}
	case "/add":
		return botCommand{Kind: KindAdd, Codes: extractFundCodes(tail)}
	case "/remove", "/remover":
		return botCommand{Kind: KindRemove, Codes: extractFundCodes(tail)}
	case "/documentos", "/docs":
		code, limit := parseDocumentosArgs(tail)
		return botCommand{Kind: KindDocumentos, Code: code, Limit: limit}
	case "/pesquisa":
		code := ""
		codes := extractFundCodes(tail)
		if len(codes) > 0 {
			code = codes[0]
		}
		if code == "" {
			return botCommand{Kind: KindHelp}
		}
		return botCommand{Kind: KindPesquisa, Code: code}
	case "/cotation", "/contation", "/cotacao":
		code := ""
		codes := extractFundCodes(tail)
		if len(codes) > 0 {
			code = codes[0]
		}
		if code == "" {
			return botCommand{Kind: KindHelp}
		}
		return botCommand{Kind: KindCotation, Code: code}
	case "/rank":
		rest := strings.TrimSpace(tail)
		if strings.HasPrefix(strings.ToLower(rest), "hoje") {
			rest = strings.TrimSpace(rest[4:])
			return botCommand{Kind: KindRankHoje, Codes: extractFundCodes(rest)}
		}
		return botCommand{Kind: KindHelp}
	case "/rankhoje":
		return botCommand{Kind: KindRankHoje, Codes: extractFundCodes(tail)}
	case "/rankv":
		return botCommand{Kind: KindRankV, Codes: extractFundCodes(tail)}
	default:
		return botCommand{Kind: KindHelp}
	}
}

func extractFundCodes(text string) []string {
	matches := codeInTextRe.FindAllString(text, -1)
	set := map[string]struct{}{}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		code := strings.ToUpper(strings.TrimSpace(m))
		if _, ok := set[code]; ok {
			continue
		}
		set[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func parseDocumentosArgs(tail string) (string, int) {
	parts := strings.Fields(strings.TrimSpace(tail))
	code := ""
	limit := 0
	for _, p := range parts {
		up := strings.ToUpper(strings.TrimSpace(p))
		if code == "" {
			if _, ok := fii.ValidateFundCode(up); ok {
				code = up
				continue
			}
		}
		if limit == 0 {
			if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 0 {
				limit = n
			}
		}
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 30 {
		limit = 30
	}
	return code, limit
}

func ParseCallback(data string) (kind CommandKind, token string, ok bool) {
	data = strings.TrimSpace(data)
	if data == "" {
		return "", "", false
	}
	m := callbackRe.FindStringSubmatch(data)
	if len(m) == 0 {
		return "", "", false
	}
	kind = CommandKind(m[1])
	if len(m) >= 3 {
		token = strings.TrimSpace(m[2])
	}
	return kind, token, true
}
