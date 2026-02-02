export interface CotationTodayItem {
  price: number;
  hour: string;
}

export type CotationsTodayData = CotationTodayItem[];

export function normalizeCotationsToday(raw: any): CotationsTodayData {
  if (Array.isArray(raw)) return canonicalizeCotationsToday(normalizeStatusInvest(raw));

  const real = raw?.real;
  if (!Array.isArray(real) || real.length === 0) return [];

  const out: CotationsTodayData = new Array(real.length);
  for (let i = 0; i < real.length; i++) {
    const item: any = real[i];
    out[i] = { price: item?.price, hour: formatHour(item?.created_at) };
  }
  return canonicalizeCotationsToday(out);
}

function normalizeStatusInvest(raw: any[]): CotationsTodayData {
  if (raw.length === 0) return [];

  const realEntry =
    raw.find((x: any) => x?.currencyType === 1 || x?.symbol === 'R$' || x?.currency === 'Real brasileiro') ?? raw[0];
  const pricesRaw = Array.isArray(realEntry?.prices) ? realEntry.prices : [];
  if (pricesRaw.length === 0) return [];

  const out: CotationsTodayData = [];
  for (const item of pricesRaw) {
    const mapped = mapStatusInvestPriceItem(item);
    if (mapped) out.push(mapped);
  }
  return out;
}

function mapStatusInvestPriceItem(item: any): CotationTodayItem | null {
  const price = extractPrice(item);
  if (!Number.isFinite(price)) return null;
  const hour = formatHour(extractTime(item));
  return { price, hour };
}

function extractPrice(item: any): number {
  if (typeof item === 'number') return item;
  if (typeof item === 'string') return Number(item.replace(',', '.'));
  if (!item || typeof item !== 'object') return NaN;
  const raw =
    item.price ??
    item.value ??
    item.last ??
    item.close ??
    item.cotacao ??
    item.preco ??
    item.v;
  if (typeof raw === 'number') return raw;
  if (typeof raw === 'string') return Number(raw.replace(',', '.'));
  return NaN;
}

function extractTime(item: any): unknown {
  if (!item || typeof item !== 'object') return null;
  return (
    item.hour ??
    item.time ??
    item.date ??
    item.datetime ??
    item.created_at ??
    item.createdAt ??
    item.timestamp ??
    null
  );
}

function formatHour(dateValue: unknown): string {
  if (typeof dateValue === 'string') {
    const trimmed = dateValue.trim();
    const hhmm = trimmed.match(/\b(\d{2}:\d{2})\b/);
    if (hhmm) return hhmm[1]!;

    if (trimmed.length >= 16) {
      const sep = trimmed.charCodeAt(10);
      if ((sep === 84 || sep === 32) && trimmed.charCodeAt(13) === 58) {
        const tail = trimmed.slice(16);
        if (!tail.includes('Z') && !tail.includes('+') && !tail.includes('-')) {
          return trimmed.slice(11, 16);
        }
      }
    }
  }

  const date = new Date(typeof dateValue === 'number' ? dateValue : String(dateValue ?? ''));
  const hours = date.getHours();
  const minutes = date.getMinutes();
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return '00:00';
  return `${hours < 10 ? '0' : ''}${hours}:${minutes < 10 ? '0' : ''}${minutes}`;
}

export function canonicalizeCotationsToday(items: CotationsTodayData): CotationsTodayData {
  if (!items.length) return [];

  const byHour = new Map<string, CotationTodayItem>();

  for (const item of items) {
    const price = typeof item?.price === 'number' ? item.price : Number(item?.price);
    if (!Number.isFinite(price)) continue;
    const hour = formatHour(item?.hour);
    byHour.set(hour, { price, hour });
  }

  const out = Array.from(byHour.values());
  out.sort((a, b) => a.hour.localeCompare(b.hour));
  return out;
}
