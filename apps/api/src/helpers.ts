import type { Context } from 'hono';

type HandlerResult<T> = { data: T };

type LogLevel = 'debug' | 'info' | 'warn' | 'error';
type LogFormat = 'text' | 'json';

function normalizeLogLevel(raw: string | undefined): LogLevel {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'debug' || v === 'info' || v === 'warn' || v === 'error') return v;
  return 'info';
}

function normalizeLogFormat(raw: string | undefined): LogFormat {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'json') return 'json';
  return 'text';
}

function levelValue(level: LogLevel): number {
  if (level === 'debug') return 10;
  if (level === 'info') return 20;
  if (level === 'warn') return 30;
  return 40;
}

function nowIso(): string {
  return new Date().toISOString();
}

export function errorToMeta(err: unknown): { name?: string; message: string; stack?: string } {
  if (err instanceof Error) return { name: err.name, message: err.message, stack: err.stack };
  return { message: typeof err === 'string' ? err : JSON.stringify(err) };
}

function toKeyValueString(meta: Record<string, unknown> | undefined): string {
  if (!meta) return '';
  const parts: string[] = [];
  for (const [k, v] of Object.entries(meta)) {
    if (v === undefined) continue;
    if (v === null) {
      parts.push(`${k}=null`);
      continue;
    }
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      parts.push(`${k}=${String(v)}`);
      continue;
    }
    parts.push(`${k}=${JSON.stringify(v)}`);
  }
  return parts.length ? ` ${parts.join(' ')}` : '';
}

export function createLoggerFromEnv(env: NodeJS.ProcessEnv = process.env) {
  const minLevel = normalizeLogLevel(env.LOG_LEVEL);
  const format = normalizeLogFormat(env.LOG_FORMAT);

  function write(level: LogLevel, message: string, meta?: Record<string, unknown>) {
    if (levelValue(level) < levelValue(minLevel)) return;
    if (format === 'json') {
      const payload = { ts: nowIso(), level, msg: message, ...meta };
      process.stdout.write(`${JSON.stringify(payload)}\n`);
      return;
    }
    const line = `${nowIso()} level=${level} msg=${JSON.stringify(message)}${toKeyValueString(meta)}`;
    process.stdout.write(`${line}\n`);
  }

  return {
    debug: (message: string, meta?: Record<string, unknown>) => write('debug', message, meta),
    info: (message: string, meta?: Record<string, unknown>) => write('info', message, meta),
    warn: (message: string, meta?: Record<string, unknown>) => write('warn', message, meta),
    error: (message: string, meta?: Record<string, unknown>) => write('error', message, meta),
  };
}

export const logger = createLoggerFromEnv();

export function createHandler<T>(
  handler: (c: Context) => Promise<HandlerResult<T> | Response>,
  errorMessage: string
) {
  return async (c: Context) => {
    try {
      const result = await handler(c);
      if (result instanceof Response) return result;
      return c.json(result.data);
    } catch (error) {
      if (error instanceof Error && error.message === 'FII_NOT_FOUND') {
        return c.json({
          error: 'FII não encontrado',
          message: 'O fundo de investimento informado não existe',
        }, 404);
      }
      logger.error(errorMessage, { method: c.req.method, path: c.req.path, err: errorToMeta(error) });
      return c.json({ error: errorMessage }, 500);
    }
  };
}
