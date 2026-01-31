# FII API

API para consultar dados de Fundos de Investimento Imobiliário.

## Endpoints

- `GET /api/fii` - Lista todos os FIIs
- `GET /api/fii/{code}` - Dados básicos do FII
- `GET /api/fii/{code}/indicators` - Indicadores históricos
- `GET /api/fii/{code}/cotations` - Cotações
- `GET /api/fii/{code}/dividends` - Dividendos

## Retornos

### GET /api/fii

```json
{
  "total": 489,
  "data": [
    {
      "code": "ZIFI11",
      "sector": "Híbrido",
      "p_vp": 0.51,
      "dividend_yield": 0,
      "dividend_yield_last_5_years": 0.69,
      "daily_liquidity": 4939,
      "net_worth": 83537343.23,
      "type": "Fundo de Desenvolvimento"
    }
  ]
}
```

### GET /api/fii/{code}

```json
{
  "id": "474",
  "code": "ZAVI11",
  "razao_social": "FUNDO DE INVESTIMENTO IMOBILIÁRIO ZAVIT REAL ESTATE",
  "cnpj": "40575940000123",
  "publico_alvo": "Geral",
  "mandato": "Renda",
  "segmento": "Híbrido",
  "tipo_fundo": "Fundo Misto",
  "prazo_duracao": "Indeterminado",
  "tipo_gestao": "Ativa",
  "taxa_adminstracao": "1,25% a.a.",
  "vacancia": 0.9,
  "numero_cotistas": 2405,
  "cotas_emittidas": 11733209,
  "valor_patrimonial_cota": 13.57,
  "valor_patrimonial": 159180000,
  "ultimo_rendimento": 0.11
}
```

**Observações:**
- `cnpj`: Remove pontos, traços e barras
- `vacancia`: Remove `%` e converte para number
- `numero_cotistas`: Remove pontos de milhar
- `cotas_emittidas`: Remove pontos de milhar
- `valor_patrimonial_cota`: Remove `R$` e formata para number
- `valor_patrimonial`: Remove `R$`, detecta sufixos (Milhões, Bilhões, Mil) e converte para number
- `ultimo_rendimento`: Remove `R$` e formata para number

### GET /api/fii/{code}/cotations

```json
{
  "real": [
    { "price": 10.21, "date": "23/01/2026" },
    { "price": 10.21, "date": "24/01/2026" }
  ],
  "dolar": [
    { "price": 1.94, "date": "23/01/2026" }
  ],
  "euro": [
    { "price": 1.65, "date": "23/01/2026" }
  ]
}
```

### GET /api/fii/{code}/dividends

```json
[
  { "value": 1.11, "yield": 1.04, "date": "11/2025" },
  { "value": 1.3, "yield": 1.21, "date": "12/2025" },
  { "value": 1.2, "yield": 1.12, "date": "01/2026" }
]
```

### GET /api/fii/{code}/indicators

```json
{
  "p_vp": [
    { "year": "Atual", "value": 0.51 },
    { "year": "2025", "value": 0.51 }
  ],
  "dividend_yield": [
    { "year": "Atual", "value": 0 },
    { "year": "2025", "value": null }
  ]
}
```

## Variáveis de Ambiente

| Variável | Descrição |
|----------|-----------|
| `COOKIE` | Cookie da sessão do investidor10 (obter do navegador) |
| `PORT`   | Porta do servidor (padrão: 3000) |

## Rodar o projeto

```bash
npm install
npm run dev
```
