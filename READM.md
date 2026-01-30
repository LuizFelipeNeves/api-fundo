## Lista todos os FII

curl -L 'https://investidor10.com.br/api/fii/advanced-search' \
-H 'accept: application/json' \
-H 'accept-language: pt-BR,pt;q=0.9' \
-H 'content-type: application/x-www-form-urlencoded; charset=UTF-8' \
-H 'origin: https://investidor10.com.br' \
-H 'priority: u=1, i' \
-H 'referer: https://investidor10.com.br/fiis/busca-avancada/' \
-H 'sec-ch-ua: "Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"' \
-H 'sec-ch-ua-mobile: ?0' \
-H 'sec-ch-ua-platform: "macOS"' \
-H 'sec-fetch-dest: empty' \
-H 'sec-fetch-mode: cors' \
-H 'sec-fetch-site: same-origin' \
-H 'sec-gpc: 1' \
-H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36' \
-H 'x-csrf-token: CTGmgCUHY62gqvsBGnHJRUWtuRZhmLw5WXQNPjBn' \
-H 'x-requested-with: XMLHttpRequest' \
-d 'draw=2&columns%5B0%5D%5Bdata%5D=&columns%5B0%5D%5Bname%5D=name&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=p_vp&columns%5B1%5D%5Bname%5D=p_vp&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=dividend_yield&columns%5B2%5D%5Bname%5D=dividend_yield&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=dividend_yield_last_5_years&columns%5B3%5D%5Bname%5D=dividend_yield_last_5_years&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=daily_liquidity&columns%5B4%5D%5Bname%5D=daily_liquidity&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=net_worth&columns%5B5%5D%5Bname%5D=net_worth&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=type&columns%5B6%5D%5Bname%5D=type&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=sector&columns%5B7%5D%5Bname%5D=sector&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=1000&search%5Bvalue%5D=&search%5Bregex%5D=false&type_page=fiis&sector=&type=&ranges%5Bp_vp%5D%5B0%5D=0&ranges%5Bp_vp%5D%5B1%5D=100&ranges%5Bp_vp%5D%5B2%5D=1&ranges%5Bp_vp%5D%5B3%5D=1&ranges%5Bdividend_yield%5D%5B0%5D=0&ranges%5Bdividend_yield%5D%5B1%5D=100&ranges%5Bdividend_yield%5D%5B2%5D=1&ranges%5Bdividend_yield%5D%5B3%5D=1&ranges%5Bdividend_yield_last_5_years%5D%5B0%5D=0&ranges%5Bdividend_yield_last_5_years%5D%5B1%5D=100&ranges%5Bdividend_yield_last_5_years%5D%5B2%5D=0&ranges%5Bdividend_yield_last_5_years%5D%5B3%5D=1&daily_liquidity=&net_worth='


##  Pega os dados basicos do FII e code = binc11
GET https://investidor10.com.br/fiis/:code/

Retorna html >>> 

## Id do FII = 631
<button type="button" class="btn-default black" id="btn-rate-fii" data-company-id="631">Avaliar</button>

## Dados basicos
Analisa o table.html

## Busca os indicadores do FII = id = 631
https://investidor10.com.br/api/fii/historico-indicadores/:id/5

## Busca cotacoes
https://investidor10.com.br/api/fii/cotacoes/chart/:id/1825/true

## Busca dividendos
https://investidor10.com.br/api/fii/dividendos/chart/:id/1825/mes

## Busca dividendos yield
https://investidor10.com.br/api/fii/dividend-yield/chart/:id/1825/mes
