package httpapi

import "net/http"

func openapiSpec() map[string]any {
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":   "go-api",
			"version": "0.1.0",
		},
		"paths": map[string]any{
			"/": map[string]any{
				"get": map[string]any{
					"summary": "Health check",
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
			"/api/telegram/webhook": map[string]any{
				"post": map[string]any{
					"summary": "Telegram webhook receiver",
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
			"/api/fii/": map[string]any{
				"get": map[string]any{
					"summary": "List funds",
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}": map[string]any{
				"get": map[string]any{
					"summary":    "Fund details",
					"parameters": []any{pathParamFundCode()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/indicators": map[string]any{
				"get": map[string]any{
					"summary":    "Latest indicators snapshot",
					"parameters": []any{pathParamFundCode()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/cotations": map[string]any{
				"get": map[string]any{
					"summary":    "Historical cotations",
					"parameters": []any{pathParamFundCode(), queryParamDays()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/dividends": map[string]any{
				"get": map[string]any{
					"summary":    "Dividends",
					"parameters": []any{pathParamFundCode()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/cotations-today": map[string]any{
				"get": map[string]any{
					"summary":    "Today cotations snapshot",
					"parameters": []any{pathParamFundCode()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/documents": map[string]any{
				"get": map[string]any{
					"summary":    "Documents",
					"parameters": []any{pathParamFundCode()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/api/fii/{code}/export": map[string]any{
				"get": map[string]any{
					"summary":    "Aggregated export",
					"parameters": []any{pathParamFundCode(), queryParamCotationsDays(), queryParamIndicatorsSnapshotsLimit()},
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
						"400": map[string]any{"description": "Invalid code"},
						"404": map[string]any{"description": "Not found"},
						"500": map[string]any{"description": "Internal error"},
					},
				},
			},
			"/openapi.json": map[string]any{
				"get": map[string]any{
					"summary": "OpenAPI 3.0 spec",
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
			"/docs/": map[string]any{
				"get": map[string]any{
					"summary": "Swagger UI",
					"responses": map[string]any{
						"200": map[string]any{"description": "OK"},
					},
				},
			},
		},
	}
}

func pathParamFundCode() map[string]any {
	return map[string]any{
		"name":     "code",
		"in":       "path",
		"required": true,
		"schema":   map[string]any{"type": "string", "example": "binc11"},
	}
}

func queryParamDays() map[string]any {
	return map[string]any{
		"name":        "days",
		"in":          "query",
		"required":    false,
		"description": "How many days to return (max 5000)",
		"schema":      map[string]any{"type": "integer", "example": 1825},
	}
}

func queryParamCotationsDays() map[string]any {
	return map[string]any{
		"name":        "cotationsDays",
		"in":          "query",
		"required":    false,
		"description": "How many cotation days to include",
		"schema":      map[string]any{"type": "integer", "example": 1825},
	}
}

func queryParamIndicatorsSnapshotsLimit() map[string]any {
	return map[string]any{
		"name":        "indicatorsSnapshotsLimit",
		"in":          "query",
		"required":    false,
		"description": "How many indicator snapshots to include",
		"schema":      map[string]any{"type": "integer", "example": 365},
	}
}

func swaggerUIHTML(openapiURL string) string {
	return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>go-api docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
    <style>html,body{margin:0;padding:0}</style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.ui = SwaggerUIBundle({
        url: "` + openapiURL + `",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis],
        layout: "BaseLayout"
      });
    </script>
  </body>
</html>`
}

func allowOnlyGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("allow", http.MethodGet)
	http.NotFound(w, r)
	return false
}
