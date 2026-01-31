export interface DividendData {
  value: number;
  yield: number;
  date: string;
}

export function normalizeDividends(
  dividends: any[],
  dividendYield: any[]
): DividendData[] {
  const dividendMap = new Map<string, number>();
  for (const item of dividendYield) {
    dividendMap.set(item.created_at, item.price);
  }

  const result: DividendData[] = [];
  for (const item of dividends) {
    result.push({
      value: item.price,
      yield: dividendMap.get(item.created_at) ?? 0,
      date: item.created_at,
    });
  }

  return result;
}
