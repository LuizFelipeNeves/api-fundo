# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FII API - A TypeScript API that scrapes and serves data about Brazilian Real Estate Investment Funds (Fundos de Investimento Imobiliário) from investidor10.com.br. Built with Hono framework with OpenAPI/Swagger documentation.

## Commands

```bash
npm install          # Install dependencies
npm run dev          # Start dev server with hot reload (tsx watch)
npm run build        # TypeScript build (tsc)
npm start            # Run production server (node dist/index.js)
```

## Architecture

```
src/
├── index.ts           # App entry point - mounts routers, OpenAPI doc, Swagger UI
├── routes/fii.ts      # All /api/fii/* routes with OpenAPI definitions
├── openapi-schemas.ts # Zod schemas for request/response validation
├── services/client.ts # Business logic - fetch data from investidor10.com.br
├── parsers/           # Data normalization (scraped HTML/JSON -> clean objects)
│   ├── fii-details.ts # Normalize FII details (vacancy %, CNPJ, currency formats)
│   ├── cotations.ts   # Normalize historical cotations
│   ├── today.ts       # Normalize intraday cotations
│   ├── dividends.ts   # Normalize dividend data
│   └── indicators.ts  # Normalize historical indicators
├── http/client.ts     # HTTP client with 10s timeout, get/post/fetchText
├── helpers.ts         # Handler wrapper with error handling (FII_NOT_FOUND -> 404)
├── config/            # Base URL, headers, CSRF token, cookies
└── types/             # TypeScript interfaces
```

## Key Patterns

- **FII Code Validation**: All routes require codes matching `/^[A-Za-z]{4}11$/` (e.g., `BINC11`, `ZAVI11`)
- **Error Handling**: `helpers.ts` catches `FII_NOT_FOUND` errors (HTTP 410 from source) and returns 404
- **Number Parsing**: Brazilian formats (R$, vírgula decimal, Milhões/Bilhões suffixes) handled in parsers
- **Date Formats**: Output dates are `dd/MM/yyyy` or `dd/MM/yyyy HH:mm` for intraday

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/fii` | List all FIIs |
| GET | `/api/fii/{code}` | FII details (404 if not found) |
| GET | `/api/fii/{code}/indicators` | Historical indicators |
| GET | `/api/fii/{code}/cotations` | Historical cotations (real/dolar/euro) |
| GET | `/api/fii/{code}/cotations-today` | Intraday cotations |
| GET | `/api/fii/{code}/dividends` | Dividend history |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `COOKIE` | Session cookie from investidor10.com.br (required for API) |
| `PORT` | Server port (default: 3000) |

## Configuration

Source API: `investidor10.com.br` - cookies and CSRF token in `src/config/index.ts` must be updated periodically as they expire.
