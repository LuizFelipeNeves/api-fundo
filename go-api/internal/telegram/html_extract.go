package telegram

import (
	"html"
	"strings"

	nethtml "golang.org/x/net/html"
)

func normalizeSpaces(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}

func cleanKey(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ":")
	return s
}

func validPair(k, v string) bool {

	if k == "" || v == "" {
		return false
	}

	if k == v {
		return false
	}

	// ignora headers comuns FNET
	lk := strings.ToLower(k)

	if lk == "rendimento" ||
		lk == "amortizacao" ||
		lk == "amortização" {
		return false
	}

	if len(k) > 120 {
		return false
	}

	return true
}

func isLikelyLabel(k string) bool {

	lk := strings.ToLower(k)

	// heurística FNET
	return strings.Contains(lk, "data") ||
		strings.Contains(lk, "codigo") ||
		strings.Contains(lk, "nome") ||
		strings.Contains(lk, "valor") ||
		strings.Contains(lk, "periodo") ||
		strings.Contains(lk, "cnpj")
}

func putIfAbsent(m map[string]string, k, v string) {
	if _, ok := m[k]; ok {
		return
	}
	m[k] = v
}

func elementName(n *nethtml.Node) string {
	if n == nil || n.Type != nethtml.ElementNode {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(n.Data))
}

func nodeText(root *nethtml.Node) string {
	var b strings.Builder

	var walk func(n *nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n == nil {
			return
		}

		if n.Type == nethtml.ElementNode {
			switch elementName(n) {
			case "script", "style":
				return
			case "br", "p", "div", "li":
				b.WriteByte(' ')
			}
		}

		if n.Type == nethtml.TextNode {
			b.WriteString(n.Data)
			b.WriteByte(' ')
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(root)
	return normalizeSpaces(html.UnescapeString(b.String()))
}

func collectRowCells(tr *nethtml.Node) []string {
	if tr == nil {
		return nil
	}
	cells := make([]string, 0, 8)
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		switch elementName(c) {
		case "td", "th":
			txt := nodeText(c)
			if txt != "" {
				cells = append(cells, txt)
			}
		}
	}
	return cells
}

func processTable(table *nethtml.Node, out map[string]string) {
	var valueColLabels []string

	processLine := func(row []string) {
		if len(row) < 2 {
			return
		}

		if len(row) >= 2 {
			a := strings.ToLower(normalizeSpaces(row[len(row)-2]))
			b := strings.ToLower(normalizeSpaces(row[len(row)-1]))
			if strings.Contains(a, "rendimento") && strings.Contains(b, "amortiza") {
				valueColLabels = []string{normalizeSpaces(row[len(row)-2]), normalizeSpaces(row[len(row)-1])}
			}
		}

		for i := 0; i+1 < len(row); i += 2 {
			key := cleanKey(row[i])
			val := row[i+1]
			if validPair(key, val) {
				putIfAbsent(out, key, val)
			}
		}

		if len(row) == 2 {
			key := cleanKey(row[0])
			val := row[1]
			if validPair(key, val) {
				putIfAbsent(out, key, val)
			}
		}

		if len(row) == 3 && len(valueColLabels) == 2 {
			baseKey := cleanKey(row[0])
			secondVal := row[2]
			if baseKey != "" && strings.TrimSpace(secondVal) != "" {
				suffix := cleanKey(valueColLabels[1])
				if suffix != "" {
					key2 := baseKey + " (" + suffix + ")"
					if validPair(key2, secondVal) {
						putIfAbsent(out, key2, secondVal)
					}
				}
			}
		}

		if len(row) >= 3 {
			for i := 0; i < len(row)-1; i++ {
				k := cleanKey(row[i])
				v := row[i+1]
				if isLikelyLabel(k) && validPair(k, v) {
					putIfAbsent(out, k, v)
				}
			}
		}
	}

	var walk func(n *nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n == nil {
			return
		}

		if n != table && elementName(n) == "table" {
			return
		}

		if elementName(n) == "tr" {
			row := collectRowCells(n)
			if len(row) > 0 {
				processLine(row)
			}
			return
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(table)
}

// ===============================
// WAR MODE extractor
// ===============================
func extractHTMLTablePairs(rawHTML string) map[string]string {

	out := map[string]string{}

	doc, err := nethtml.Parse(strings.NewReader(rawHTML))
	if err != nil || doc == nil {
		return out
	}

	var walk func(n *nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n == nil {
			return
		}
		if elementName(n) == "table" {
			processTable(n, out)
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}

	walk(doc)

	return out
}
