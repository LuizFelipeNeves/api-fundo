package telegram

import (
	"html"
	"regexp"
	"strings"
)

var htmlStripScriptStyleRe = regexp.MustCompile(`(?is)<(script|style)[^>]*>.*?</\1>`)
var htmlTableRe = regexp.MustCompile(`(?is)<table[^>]*>.*?</table>`)
var htmlTrRe = regexp.MustCompile(`(?is)<tr[^>]*>.*?</tr>`)
var htmlCellRe = regexp.MustCompile(`(?is)<t[dh][^>]*>.*?</t[dh]>`)
var htmlTagRe = regexp.MustCompile(`(?is)<[^>]+>`)

func extractHTMLTablePairs(rawHTML string) map[string]string {
	clean := htmlStripScriptStyleRe.ReplaceAllString(rawHTML, "")
	out := map[string]string{}

	tables := htmlTableRe.FindAllString(clean, -1)
	for _, table := range tables {
		rows := htmlTrRe.FindAllString(table, -1)
		for _, row := range rows {
			cells := htmlCellRe.FindAllString(row, -1)
			if len(cells) == 0 {
				continue
			}
			line := make([]string, 0, len(cells))
			for _, cell := range cells {
				text := html.UnescapeString(htmlTagRe.ReplaceAllString(cell, " "))
				text = strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
				if text != "" {
					line = append(line, text)
				}
			}
			for i := 0; i+1 < len(line); i++ {
				key := strings.TrimSpace(strings.TrimSuffix(line[i], ":"))
				val := strings.TrimSpace(line[i+1])
				if key == "" || val == "" {
					continue
				}
				if len(key) >= 100 {
					continue
				}
				if key == val {
					continue
				}
				out[key] = val
				i++
			}
		}
	}
	return out
}
