# Ideias / rascunhos

Conteúdo solto e links usados durante exploração.

## Links

- https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo=42273325000198
- https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id=235276&cvm=true&

## GEMINI

```json
{
  "analise_de_ativos_e_risco": {
    "alocacao_por_ativo": [
      { "classe": "CRA", "exposicao_financeira": 140188353.93, "peso_pl": 51.2 },
      { "classe": "Cotas FIAGRO", "exposicao_financeira": 73996578.83, "peso_pl": 27.0 },
      { "classe": "Liquidez/Caixa", "exposicao_financeira": 59940973.6, "peso_pl": 21.8 }
    ],
    "perfil_indexador": { "observacao": "Extrair se disponível (ex: % CDI, % IPCA)" },
    "duration_media_carteira": "Acima de 1080 dias",
    "is_qualificado": true
  },
  "engine_alertas_inteligentes": [
    {
      "id": "ALERTA_001",
      "tag": "DESENQUADRAMENTO_OU_CAIXA",
      "mensagem": "Nível de caixa elevado (21.8%). Possível 'cash drag' ou preparação para resgate/amortização.",
      "impacto": "MÉDIO"
    },
    {
      "id": "ALERTA_002",
      "tag": "RETORNO_CAPITAL",
      "mensagem": "Amortização de R$ 30,00 reduz base de custo e patrimônio total do fundo.",
      "impacto": "ALTO"
    },
    {
      "id": "ALERTA_003",
      "tag": "RENTABILIDADE_NEGATIVA",
      "mensagem": "Rentabilidade patrimonial de -3.54% sugere marcação a mercado negativa dos ativos de crédito.",
      "impacto": "MÉDIO"
    }
  ],
  "insights_ia": "O fundo está em fase de devolução de capital ou reciclagem de portfólio. A ausência de rendimentos em nov/25 seguida de amortização em jan/26 sugere gestão ativa de liquidez."
}
```

## Comunicado de dividendos e amortizações

```json
{
  "ticker": "X",
  "nome_do_fundo": "X",
  "data_base": "30/01/2026",
  "data_de_pagamento": "06/02/2026",
  "periodo_de_referencia": "01/2026",
  "dividendos": 1.1,
  "amortizacao": 0.0
}
```

