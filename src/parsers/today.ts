export interface CotationTodayItem {
  price: number;
  hour: string;
}

export type CotationsTodayData = CotationTodayItem[];

export function normalizeCotationsToday(raw: Record<string, any[]>): CotationsTodayData {
  const real = raw.real;
  if (!Array.isArray(real) || real.length === 0) return [];

  const out: CotationsTodayData = new Array(real.length);
  for (let i = 0; i < real.length; i++) {
    const item: any = real[i];
    out[i] = {
      price: item?.price,
      hour: formatDate(item?.created_at),
    };
  }
  return out;
}

function formatDate(dateStr: string): string {
  if (typeof dateStr === 'string' && dateStr.length >= 16) {
    const sep = dateStr.charCodeAt(10);
    if ((sep === 84 || sep === 32) && dateStr.charCodeAt(13) === 58) {
      const tail = dateStr.slice(16);
      if (!tail.includes('Z') && !tail.includes('+') && !tail.includes('-')) {
        return dateStr.slice(11, 16);
      }
    }
  }

  const date = new Date(dateStr);
  const hours = date.getHours();
  const minutes = date.getMinutes();
  return `${hours < 10 ? '0' : ''}${hours}:${minutes < 10 ? '0' : ''}${minutes}`;
}
