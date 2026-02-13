package telegram

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExtractHTMLTablePairs_RedimentosHTML(t *testing.T) {
	raw := `
		<html>
		<head>
		  <style>table{border:1px solid black}</style>
		  <script>console.log("ignore");</script>
		</head>
		<body>
			<table border="1" width="95%" align="center">
				<tr>
					<td width="20%"><span class="titulo-dado">Nome do Fundo: </span></td>
					<td width="40%"><span class="dado-cabecalho">FUNDO DE INVESTIMENTO IMOBILI&Aacute;RIO - BLUE RECEB&Iacute;VEIS IMOBILI&Aacute;RIOS</span></td>
					<td width="20%"><span class="titulo-dado">CNPJ do Fundo: </span></td>
					<td width="20%"><span class="dado-cabecalho">38.051.307/0001-94</span></td>
				</tr>
				<tr>
					<td><span class="titulo-dado">Nome do Administrador: </span></td>
					<td><span class="dado-cabecalho">BANCO DAYCOVAL S.A.</span></td>
					<td><span class="titulo-dado">CNPJ do Administrador: </span></td>
					<td><span class="dado-cabecalho">62.232.889/0001-90</span></td>
				</tr>
				<tr>
					<td><span class="titulo-dado">Data da Informa&ccedil;&atilde;o: </span></td>
					<td><span class="dado-valores">10/10/2023</span></td>
					<td><span class="titulo-dado">Ano: </span></td>
					<td><span class="dado-cabecalho">2023</span></td>
				</tr>
			</table>
			<table cellpading="5" cellspacing="5" width="95%" align="center">
				<tr>
					<td width="15%"><span class="titulo-dado">C&oacute;digo ISIN: </span></td>
					<td width="15%"><span class="dado-cabecalho">BRBLURCTF005</span></td>
					<td width="15%"><span class="titulo-dado">C&oacute;digo de negocia&ccedil;&atilde;o: </span></td>
					<td width="15%"><span class="dado-cabecalho">BLUR11</span></td>
					<td width="20%" valign="top" align="center"><b>Rendimento</b></td>
					<td width="20%" align="center"><b>Amortiza&ccedil;&atilde;o</b></td>
				</tr>
				<tr>
					<td colspan="4">Data-base (&uacute;ltimo dia de negocia&ccedil;&atilde;o &ldquo;com&rdquo; direito ao provento)</td>
					<td><span class="dado-valores">10/10/2023</span></td>
					<td></td>
				</tr>
				<tr>
					<td colspan="4">Valor do provento (R$/unidade)</td>
					<td><span class="dado-valores">1,03</span></td>
					<td></td>
				</tr>
				<tr>
					<td colspan="4">Per&iacute;odo de refer&ecirc;ncia</td>
					<td><span class="dado-valores">Setembro</span></td>
					<td><span class="dado-valores"></span></td>
				</tr>
				<tr>
					<td colspan="4">Rendimento isento de IR*</td>
					<td><span class="dado-valores">Sim</span></td>
				</tr>
			</table>
		</body>
		</html>
	`

	got := extractHTMLTablePairs(raw)

	if b, err := json.MarshalIndent(got, "", "  "); err == nil {
		t.Logf("extracted_json=%s", string(b))
	}

	if got["Nome do Fundo"] != "FUNDO DE INVESTIMENTO IMOBILIÁRIO - BLUE RECEBÍVEIS IMOBILIÁRIOS" {
		t.Fatalf("Nome do Fundo: got=%q", got["Nome do Fundo"])
	}
	if got["CNPJ do Fundo"] != "38.051.307/0001-94" {
		t.Fatalf("CNPJ do Fundo: got=%q", got["CNPJ do Fundo"])
	}
	if got["Código ISIN"] != "BRBLURCTF005" {
		t.Fatalf("Código ISIN: got=%q", got["Código ISIN"])
	}
	if got["Código de negociação"] != "BLUR11" {
		t.Fatalf("Código de negociação: got=%q", got["Código de negociação"])
	}
	if got["Valor do provento (R$/unidade)"] != "1,03" {
		t.Fatalf("Valor do provento: got=%q", got["Valor do provento (R$/unidade)"])
	}
	if got["Período de referência"] != "Setembro" {
		t.Fatalf("Período de referência: got=%q", got["Período de referência"])
	}
	if got["Rendimento isento de IR*"] != "Sim" {
		t.Fatalf("Rendimento isento de IR*: got=%q", got["Rendimento isento de IR*"])
	}

	foundDataBase := false
	for k, v := range got {
		if strings.HasPrefix(k, "Data-base") {
			foundDataBase = true
			if v != "10/10/2023" {
				t.Fatalf("Data-base: key=%q got=%q", k, v)
			}
		}
	}
	if !foundDataBase {
		t.Fatalf("expected a Data-base key, got keys=%v", sortedKeys(got))
	}
}

func TestExtractHTMLTablePairs_RedimentosHTML_ComAmortizacao(t *testing.T) {
	raw := `
		<html>
		<body>
			<table cellpading="5" cellspacing="5" width="95%" align="center">
				<tr>
					<td width="15%"><span class="titulo-dado">C&oacute;digo ISIN: </span></td><td width="15%"><span class="dado-cabecalho">BRBLURCTF005</span></td><td width="15%"><span class="titulo-dado">C&oacute;digo de negocia&ccedil;&atilde;o: </span></td><td width="15%"><span class="dado-cabecalho">BLUR11</span></td><td width="20%" valign="top" align="center"><b>Rendimento</b></td><td width="20%" align="center"><b>Amortiza&ccedil;&atilde;o</b><span class="dado-cabecalho">(Total)</span></td>
				</tr>
				<tr>
					<td colspan="4">Data-base (&uacute;ltimo dia de negocia&ccedil;&atilde;o &ldquo;com&rdquo; direito ao provento)</td><td><span class="dado-valores">07/07/2025</span></td><td><span class="dado-valores">07/07/2025</span></td>
				</tr>
				<tr>
					<td colspan="4">Valor do provento (R$/unidade)</td><td><span class="dado-valores">1,01065</span></td><td><span class="dado-valores">74,830185</span></td>
				</tr>
				<tr>
					<td colspan="4">Data do pagamento</td><td><span class="dado-valores">14/07/2025</span></td><td><span class="dado-valores">14/07/2025</span></td>
				</tr>
			</table>
		</body>
		</html>
	`

	got := extractHTMLTablePairs(raw)

	if b, err := json.MarshalIndent(got, "", "  "); err == nil {
		t.Logf("extracted_json_amortizacao=%s", string(b))
	}

	if got["Valor do provento (R$/unidade)"] != "1,01065" {
		t.Fatalf("Valor do provento (R$/unidade): got=%q", got["Valor do provento (R$/unidade)"])
	}
	if got["Valor do provento (R$/unidade) (Amortização (Total))"] != "74,830185" {
		t.Fatalf("Valor do provento amortização: got=%q", got["Valor do provento (R$/unidade) (Amortização (Total))"])
	}
	foundDataBase := false
	for k, v := range got {
		if strings.HasPrefix(k, "Data-base") && v == "07/07/2025" {
			foundDataBase = true
			break
		}
	}
	if !foundDataBase {
		t.Fatalf("expected Data-base key with 07/07/2025, got keys=%v", sortedKeys(got))
	}
	if got["Data do pagamento"] != "14/07/2025" {
		t.Fatalf("Data do pagamento: got=%q", got["Data do pagamento"])
	}
	if got["Data do pagamento (Amortização (Total))"] != "14/07/2025" {
		t.Fatalf("Data do pagamento amortização: got=%q", got["Data do pagamento (Amortização (Total))"])
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}
