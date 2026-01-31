import { getDefaultHeaders } from '../config';

const DEFAULT_TIMEOUT = 10000; // 10 segundos

interface RequestOptions {
  timeout?: number;
}

async function readResponseSnippet(response: Response, maxChars: number): Promise<string> {
  try {
    const text = await response.text();
    const trimmed = text.replace(/\s+/g, ' ').trim();
    if (!trimmed) return '';
    return trimmed.length > maxChars ? `${trimmed.slice(0, maxChars)}…` : trimmed;
  } catch {
    return '';
  }
}

async function throwHttpError(response: Response, method: string, url: string): Promise<never> {
  const statusText = response.statusText ? ` ${response.statusText}` : '';
  const contentType = response.headers.get('content-type') || '';
  const snippet = await readResponseSnippet(response, 800);
  const bodyPart = snippet ? ` body="${snippet}"` : '';
  const typePart = contentType ? ` content_type="${contentType}"` : '';
  throw new Error(`${method} ${url} -> HTTP ${response.status}${statusText}${typePart}${bodyPart}`);
}

function extractCookieNameValuesFromSetCookieHeader(setCookieHeader: string): string[] {
  const cookies: string[] = [];
  for (const match of setCookieHeader.matchAll(/(?:^|,\s*)([^=;, \t]+=[^;]+)/g)) {
    const nameValue = match[1]?.trim();
    if (nameValue) cookies.push(nameValue);
  }
  return cookies;
}

async function request<T>(
  url: string,
  options: RequestOptions = {}
): Promise<T> {
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      headers: getDefaultHeaders(),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      await throwHttpError(response, 'GET', url);
    }

    return response.json() as Promise<T>;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`GET ${url} -> timeout_after_ms=${timeout}`);
    }
    throw error;
  }
}

export async function get<T>(url: string, options?: RequestOptions): Promise<T> {
  return request<T>(url, options);
}

export async function post<T>(
  url: string,
  body: string,
  options?: RequestOptions
): Promise<T> {
  const timeout = options?.timeout ?? DEFAULT_TIMEOUT;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: getDefaultHeaders(),
      body,
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      await throwHttpError(response, 'POST', url);
    }

    return response.json() as Promise<T>;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`POST ${url} -> timeout_after_ms=${timeout}`);
    }
    throw error;
  }
}

export async function fetchText(url: string, options?: RequestOptions): Promise<string> {
  const timeout = options?.timeout ?? DEFAULT_TIMEOUT;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: getDefaultHeaders(),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (response.status === 410) {
      throw new Error('FII_NOT_FOUND');
    }

    if (!response.ok) {
      await throwHttpError(response, 'GET', url);
    }

    return response.text();
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`GET ${url} -> timeout_after_ms=${timeout}`);
    }
    throw error;
  }
}

export async function fetchWithSession<T>(
  initUrl: string,
  dataUrl: string,
  options: RequestOptions = {}
): Promise<T> {
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    // Primeira requisição para estabelecer sessão
    const initResponse = await fetch(initUrl, {
      method: 'GET',
      headers: {
        ...getDefaultHeaders(),
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      signal: controller.signal,
    });

    if (!initResponse.ok) {
      clearTimeout(timeoutId);
      await throwHttpError(initResponse, 'GET', initUrl);
    }

    // Extrair cookies da primeira resposta
    const cookies: string[] = [];
    const setCookies = (initResponse.headers as any).getSetCookie?.() as string[] | undefined;
    if (setCookies?.length) {
      for (const cookie of setCookies) {
        const cookieNameValue = cookie.split(';')[0]?.trim();
        if (cookieNameValue) cookies.push(cookieNameValue);
      }
    } else {
      const setCookie = initResponse.headers.get('set-cookie');
      if (setCookie) cookies.push(...extractCookieNameValuesFromSetCookieHeader(setCookie));
    }

    // Segunda requisição com os cookies
    const headers: Record<string, string> = {
      ...getDefaultHeaders(),
      'accept': 'application/json, text/javascript, */*; q=0.01',
      'x-requested-with': 'XMLHttpRequest',
    };

    if (cookies.length > 0) {
      headers['cookie'] = cookies.join('; ');
    }

    const dataResponse = await fetch(dataUrl, {
      method: 'GET',
      headers,
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!dataResponse.ok) {
      await throwHttpError(dataResponse, 'GET', dataUrl);
    }

    const json = await dataResponse.json();
    return json as T;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`GET ${dataUrl} -> timeout_after_ms=${timeout}`);
    }
    throw error;
  }
}
