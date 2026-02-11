import crypto from 'node:crypto';

export function nowIso(): string {
  return new Date().toISOString();
}

export function sha256(value: string): string {
  return crypto.createHash('sha256').update(value).digest('hex');
}

export function toDateIsoFromBr(dateStr: string): string {
  const str = String(dateStr || '').trim();
  const match = str.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!match) return '';
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = Number(match[3]);
  if (!day || !month || !year) return '';
  return new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0)).toISOString().slice(0, 10);
}
