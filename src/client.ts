const BASE_URL = 'https://investidor10.com.br';

// Cookie from browser - obter em https://investidor10.com.br/fiis/busca-avancada/
const COOKIE = process.env.COOKIE || 'XSRF-TOKEN=eyJpdiI6InNSa25kbjBBai9Oc0xKUFlrU1NwN0E9PSIsInZhbHVlIjoiNnc0SElMV2dUSzRTNjVBNktrcW1iRHBNRUp4cFhyKzNQUnZqSnR4RnVGMEwyWVJPalZOVHFSczRMVHJvWFl5MTJBWGYyZjVwbjM2MFhyS00ybnIxUlp3TTF4Nzh0dFp2NGdaTVN0K1BWMzhrRzVmdXJZajBvRkNmTmo4UXdndjkiLCJtYWMiOiJjODJmNDcwOGI1YTA4NDdkMDYwOTE2OWRiNGIyNTUwNDU5MGJlNTczZDNjMGNmZDMzN2EzZjFlZmYxYzViNDM0IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6IklGTzFWazBJN3RjSVIrQnF1by9adHc9PSIsInZhbHVlIjoieWlidUp0a0psUGVhQ0hwUXRRM1NTYkZ1ZldQc2dETjlwZFl0aHJ5RENLNXBVQndqYkpjaytrWFhwTDlIamFwMTNRVjRpbnhiUS9UV0FFR3BjVS9malljTFZLMmp2dHV0NStqck1xbnFFOTFzZ1VlenpZc2RScWtaMHd4RExzUHMiLCJtYWMiOiJmODk1YmNiZGM3MWNlNzY4ZTllODY2YTFlY2I0MDMwMzE1ZTk1NjUwYjc1NTdlYTA2ZmE5Nzk4ZDM2MzE0YjZhIiwidGFnIjoiIn0%3D';

function getDefaultHeaders() {
  return {
    'accept': 'application/json',
    'accept-language': 'pt-BR,pt;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': BASE_URL,
    'referer': `${BASE_URL}/fiis/busca-avancada/`,
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'x-csrf-token': 'CTGmgCUHY62gqvsBGnHJRUWtuRZhmLw5WXQNPjBn',
    'x-requested-with': 'XMLHttpRequest',
    'cookie': COOKIE,
  };
}

export async function fetchFIIList(): Promise<any> {
  const params = new URLSearchParams({
    draw: '2',
    'columns[0][data]': '',
    'columns[0][name]': 'name',
    'columns[0][searchable]': 'true',
    'columns[0][orderable]': 'true',
    'columns[0][search][value]': '',
    'columns[0][search][regex]': 'false',
    'columns[1][data]': 'p_vp',
    'columns[1][name]': 'p_vp',
    'columns[1][searchable]': 'true',
    'columns[1][orderable]': 'true',
    'columns[1][search][value]': '',
    'columns[1][search][regex]': 'false',
    'columns[2][data]': 'dividend_yield',
    'columns[2][name]': 'dividend_yield',
    'columns[2][searchable]': 'true',
    'columns[2][orderable]': 'true',
    'columns[2][search][value]': '',
    'columns[2][search][regex]': 'false',
    'columns[3][data]': 'dividend_yield_last_5_years',
    'columns[3][name]': 'dividend_yield_last_5_years',
    'columns[3][searchable]': 'true',
    'columns[3][orderable]': 'true',
    'columns[3][search][value]': '',
    'columns[3][search][regex]': 'false',
    'columns[4][data]': 'daily_liquidity',
    'columns[4][name]': 'daily_liquidity',
    'columns[4][searchable]': 'true',
    'columns[4][orderable]': 'true',
    'columns[4][search][value]': '',
    'columns[4][search][regex]': 'false',
    'columns[5][data]': 'net_worth',
    'columns[5][name]': 'net_worth',
    'columns[5][searchable]': 'true',
    'columns[5][orderable]': 'true',
    'columns[5][search][value]': '',
    'columns[5][search][regex]': 'false',
    'columns[6][data]': 'type',
    'columns[6][name]': 'type',
    'columns[6][searchable]': 'true',
    'columns[6][orderable]': 'true',
    'columns[6][search][value]': '',
    'columns[6][search][regex]': 'false',
    'columns[7][data]': 'sector',
    'columns[7][name]': 'sector',
    'columns[7][searchable]': 'true',
    'columns[7][orderable]': 'true',
    'columns[7][search][value]': '',
    'columns[7][search][regex]': 'false',
    start: '0',
    length: '1000',
    'search[value]': '',
    'search[regex]': 'false',
    type_page: 'fiis',
    sector: '',
    type: '',
    'ranges[p_vp][0]': '0',
    'ranges[p_vp][1]': '100',
    'ranges[p_vp][2]': '1',
    'ranges[p_vp][3]': '1',
    'ranges[dividend_yield][0]': '0',
    'ranges[dividend_yield][1]': '100',
    'ranges[dividend_yield][2]': '1',
    'ranges[dividend_yield][3]': '1',
    'ranges[dividend_yield_last_5_years][0]': '0',
    'ranges[dividend_yield_last_5_years][1]': '100',
    'ranges[dividend_yield_last_5_years][2]': '0',
    'ranges[dividend_yield_last_5_years][3]': '1',
  });

  const response = await fetch(`${BASE_URL}/api/fii/advanced-search`, {
    method: 'POST',
    headers: getDefaultHeaders(),
    body: params.toString(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export async function fetchFIIDetails(code: string): Promise<string> {
  const response = await fetch(`${BASE_URL}/fiis/${code}/`, {
    headers: getDefaultHeaders(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.text();
}

export async function fetchFIIIndicators(id: string): Promise<any> {
  const response = await fetch(`${BASE_URL}/api/fii/historico-indicadores/${id}/5`, {
    headers: getDefaultHeaders(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export async function fetchFIICotations(id: string, days: number = 1825): Promise<any> {
  const response = await fetch(`${BASE_URL}/api/fii/cotacoes/chart/${id}/${days}/true`, {
    headers: getDefaultHeaders(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export async function fetchFIIDividends(id: string, days: number = 1825): Promise<any> {
  const response = await fetch(`${BASE_URL}/api/fii/dividendos/chart/${id}/${days}/mes`, {
    headers: getDefaultHeaders(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

export async function fetchFIIDividendYield(id: string, days: number = 1825): Promise<any> {
  const response = await fetch(`${BASE_URL}/api/fii/dividend-yield/chart/${id}/${days}/mes`, {
    headers: getDefaultHeaders(),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}
