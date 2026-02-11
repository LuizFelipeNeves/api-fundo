import { toDateIsoFromBr } from '../../utils/date';

export function toMonthKeyFromBr(dateBr: string): string {
  const iso = toDateIsoFromBr(dateBr);
  if (!iso) return '';
  return iso.slice(0, 7);
}

export function monthKeyToParts(monthKey: string): { y: number; m: number } | null {
  const [ys, ms] = String(monthKey || '').split('-');
  const y = Number.parseInt(ys || '', 10);
  const m = Number.parseInt(ms || '', 10);
  if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return null;
  return { y, m };
}

export function monthKeyDiff(a: string, b: string): number {
  const ap = monthKeyToParts(a);
  const bp = monthKeyToParts(b);
  if (!ap || !bp) return 0;
  return (bp.y - ap.y) * 12 + (bp.m - ap.m);
}

export function monthKeyAdd(monthKey: string, deltaMonths: number): string {
  const p = monthKeyToParts(monthKey);
  if (!p) return '';
  const base = p.y * 12 + (p.m - 1);
  const next = base + deltaMonths;
  const y = Math.floor(next / 12);
  const m = (next % 12) + 1;
  const ys = String(y).padStart(4, '0');
  const ms = String(m).padStart(2, '0');
  return `${ys}-${ms}`;
}

export function listMonthKeysBetweenInclusive(startKey: string, endKey: string): string[] {
  const diff = monthKeyDiff(startKey, endKey);
  if (diff < 0) return [];
  const out: string[] = [];
  for (let i = 0; i <= diff; i++) out.push(monthKeyAdd(startKey, i));
  return out.filter(Boolean);
}

export function parseIsoMs(iso: string): number {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : 0;
}

export function countWeekdaysBetweenIso(startIso: string, endIso: string): number {
  const start = new Date(`${startIso}T00:00:00.000Z`);
  const end = new Date(`${endIso}T00:00:00.000Z`);
  const startMs = start.getTime();
  const endMs = end.getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return 0;
  let count = 0;
  const cursor = new Date(startMs);
  while (cursor.getTime() <= endMs) {
    const d = cursor.getUTCDay();
    if (d >= 1 && d <= 5) count++;
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return count;
}
