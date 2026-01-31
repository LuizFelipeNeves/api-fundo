export interface CotationsData {
  price: number;
  date: string;
}

export interface NormalizedCotations {
  [key: string]: CotationsData[];
}

export function normalizeCotations(raw: Record<string, any[]>): NormalizedCotations {
  const normalized: NormalizedCotations = {};

  for (const [currency, values] of Object.entries(raw)) {
    normalized[currency] = values.map((item) => ({
      price: item.price,
      date: item.created_at,
    }));
  }

  return normalized;
}
