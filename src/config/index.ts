const originName = 'investidor10';
export const BASE_URL = `https://${originName}.com.br`;

const CSRF_TOKEN = process.env.CSRF_TOKEN || 'CTGmgCUHY62gqvsBGnHJRUWtuRZhmLw5WXQNPjBn';
export const COOKIE = process.env.COOKIE || 'XSRF-TOKEN=eyJpdiI6InNSa25kbjBBai9Oc0xKUFlrU1NwN0E9PSIsInZhbHVlIjoiNnc0SElMV2dUSzRTNjVBNktrcW1iRHBNRUp4cFhyKzNQUnZqSnR4RnVGMEwyWVJPalZOVHFSczRMVHJvWFl5MTJBWGYyZjVwbjM2MFhyS00ybnIxUlp3TTF4Nzh0dFp2NGdaTVN0K1BWMzhrRzVmdXJZajBvRkNmTmo4UXdndjkiLCJtYWMiOiJjODJmNDcwOGI1YTA4NDdkMDYwOTE2OWRiNGIyNTUwNDU5MGJlNTczZDNjMGNmZDMzN2EzZjFlZmYxYzViNDM0IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6IklGTzFWazBJN3RjSVIrQnF1by9adHc9PSIsInZhbHVlIjoieWlidUp0a0psUGVhQ0hwUXRRM1NTYkZ1ZldQc2dETjlwZFl0aHJ5RENLNXBVQndqYkpjaytrWFhwTDlIamFwMTNRVjRpbnhiUS9UV0FFR3BjVS9malljTFZLMmp2dHV0NStqck1xbnFFOTFzZ1VlenpZc2RScWtaMHd4RExzUHMiLCJtYWMiOiJmODk1YmNiZGM3MWNlNzY4ZTllODY2YTFlY2I0MDMwMzE1ZTk1NjUwYjc1NTdlYTA2ZmE5Nzk4ZDM2MzE0YjZhIiwidGFnIjoiIn0%3D';

export function getDefaultHeaders() {
  return {
    'accept': 'application/json',
    'accept-language': 'pt-BR,pt;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': BASE_URL,
    'referer': `${BASE_URL}/fiis/busca-avancada/`,
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'x-csrf-token': CSRF_TOKEN,
    'x-requested-with': 'XMLHttpRequest',
    'cookie': COOKIE,
  };
}
