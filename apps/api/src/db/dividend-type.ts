export type DividendType = 'Dividendos' | 'Amortização';

export function dividendTypeFromCode(code: number): DividendType | null {
  if (code === 1) return 'Dividendos';
  if (code === 2) return 'Amortização';
  return null;
}
