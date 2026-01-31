export interface CotationTodayItem {
  price: number;
  hour: string;
}

export interface ContationsTodayData {
  real: CotationTodayItem[];
  dolar: CotationTodayItem[];
  euro: CotationTodayItem[];
}

export function normalizeCotationsToday(raw: Record<string, any[]>): ContationsTodayData {
  const normalizeArray = (items: any[]): CotationTodayItem[] => {
    return items.map((item) => ({
      price: item.price,
      hour: formatDate(item.created_at),
    }));
  };

  return {
    real: normalizeArray(raw.real || []),
    dolar: normalizeArray(raw.dollar || []),
    euro: normalizeArray(raw.euro || []),
  };
}

function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  // const day = String(date.getDate()).padStart(2, '0');
  // const month = String(date.getMonth() + 1).padStart(2, '0');
  // const year = date.getFullYear();
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  // ${day}/${month}/${year}
  return `${hours}:${minutes}`;
}
