import { getDefaultHeaders } from '../config';

const DEFAULT_TIMEOUT = 10000; // 10 segundos

interface RequestOptions {
  timeout?: number;
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
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return response.json() as Promise<T>;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
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
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return response.json() as Promise<T>;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
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
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return response.text();
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
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

    // Extrair cookies da primeira resposta
    const cookies: string[] = [];
    const setCookie = initResponse.headers.get('set-cookie');
    if (setCookie) {
      const cookieParts = setCookie.split(',');
      for (const part of cookieParts) {
        // Extrair apenas nome=valor, removendo atributos como Domain, Path, Secure, HttpOnly
        const cookieNameValue = part.split(';')[0].trim();
        if (cookieNameValue) {
          cookies.push(cookieNameValue);
        }
      }
    }

    // Segunda requisição com os cookies
    const headers: Record<string, string> = {
      ...getDefaultHeaders(),
      'accept': 'application/json, text/javascript, */*; q=0.01',
      'x-requested-with': 'XMLHttpRequest',
    };

    if (cookies.length > 0) {
      headers['Cookie'] = cookies.join('; ');
    }

    const dataResponse = await fetch(dataUrl, {
      method: 'GET',
      headers,
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!dataResponse.ok) {
      throw new Error(`HTTP error! status: ${dataResponse.status}`);
    }

    const json = await dataResponse.json();
    return json as T;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
    }
    throw error;
  }
}
