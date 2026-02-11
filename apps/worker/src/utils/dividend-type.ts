export type DividendType = 'Dividendos' | 'Amortização';

export function dividendTypeToCode(type: DividendType): number {
  if (type === 'Dividendos') return 1;
  return 2;
}
