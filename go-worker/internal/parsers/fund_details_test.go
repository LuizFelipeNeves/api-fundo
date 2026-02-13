package parsers

import "testing"

func TestExtractFundDetails_FromDescBlocksAndIDPatterns(t *testing.T) {
	html := `
		<html>
			<head>
				<script>
					window.__data = {"fii_id": 268};
				</script>
			</head>
			<body>
				<div class="desc">
					<span class="name">RAZÃO SOCIAL</span>
					<div class="value"><span>RIZA AKIN FUNDO DE INVESTIMENTO IMOBILIÁRIO</span></div>
				</div>
				<div class="desc">
					<span class="name">CNPJ</span>
					<div class="value"><span>36.642.219/0001-31</span></div>
				</div>
				<div class="desc">
					<span class="name">PÚBLICO-ALVO</span>
					<div class="value"><span>INVESTIDOR QUALIFICADO</span></div>
				</div>
				<div class="desc">
					<span class="name">MANDATO</span>
					<div class="value"><span>Títulos e valores mobiliários</span></div>
				</div>
				<div class="desc">
					<span class="name">SEGMENTO</span>
					<div class="value"><span>Títulos e Valores Mobiliários</span></div>
				</div>
				<div class="desc">
					<span class="name">TIPO DE FUNDO</span>
					<div class="value"><span>Fundo de Papel</span></div>
				</div>
				<div class="desc">
					<span class="name">PRAZO DE DURAÇÃO</span>
					<div class="value"><span>Indeterminado</span></div>
				</div>
				<div class="desc">
					<span class="name">TIPO DE GESTÃO</span>
					<div class="value"><span>Ativa</span></div>
				</div>
				<div class="desc">
					<span class="name">TAXA DE ADMINISTRAÇÃO</span>
					<div class="value"><span>0,95% a.a.</span></div>
				</div>
				<div class="desc">
					<span class="name">VACÂNCIA</span>
					<div class="value"><span>0,00%</span></div>
				</div>
				<div class="desc">
					<span class="name">NUMERO DE COTISTAS</span>
					<div class="value"><span>45.181</span></div>
				</div>
				<div class="desc">
					<span class="name">COTAS EMITIDAS</span>
					<div class="value"><span>8.807.885</span></div>
				</div>
				<div class="desc">
					<span class="name">VAL. PATRIMONIAL P/ COTA</span>
					<div class="value"><span>R$ 88,70</span></div>
				</div>
				<div class="desc">
					<span class="name">VALOR PATRIMONIAL</span>
					<div class="value"><span>R$ 780.000.000</span></div>
				</div>
				<div class="desc">
					<span class="name">ÚLTIMO RENDIMENTO</span>
					<div class="value"><span>R$ 1,20000000</span></div>
				</div>
			</body>
		</html>
	`

	got, err := ExtractFundDetails(html, "RZAK11")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.ID != "268" {
		t.Fatalf("expected ID=268, got %q", got.ID)
	}
	if got.CNPJ != "36.642.219/0001-31" {
		t.Fatalf("expected CNPJ, got %q", got.CNPJ)
	}
	if got.RazaoSocial == "" {
		t.Fatalf("expected RazaoSocial to be set")
	}
	if got.PublicoAlvo == "" || got.Mandato == "" || got.Segmento == "" || got.TipoFundo == "" || got.PrazoDuracao == "" || got.TipoGestao == "" {
		t.Fatalf("expected key string fields to be set, got %+v", got)
	}
	if got.TaxaAdministracao == 0 {
		t.Fatalf("expected TaxaAdministracao to be parsed, got %v", got.TaxaAdministracao)
	}
	if got.NumeroCotistas == nil || *got.NumeroCotistas != 45181 {
		t.Fatalf("expected NumeroCotistas=45181, got %v", got.NumeroCotistas)
	}
	if got.CotasEmitidas == nil || *got.CotasEmitidas != 8807885 {
		t.Fatalf("expected CotasEmitidas=8807885, got %v", got.CotasEmitidas)
	}
	if got.ValorPatrimonialCota == nil || *got.ValorPatrimonialCota != 88.70 {
		t.Fatalf("expected ValorPatrimonialCota=88.70, got %v", got.ValorPatrimonialCota)
	}
	if got.UltimoRendimento == nil || *got.UltimoRendimento != 1.2 {
		t.Fatalf("expected UltimoRendimento=1.2, got %v", got.UltimoRendimento)
	}
}
