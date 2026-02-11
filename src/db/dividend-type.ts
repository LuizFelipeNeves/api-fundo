export type DividendType = 'Amortização' | 'Dividendos';
export type DividendTypeCode = 1 | 2;

export function dividendTypeToCode(type: DividendType): DividendTypeCode {
  return type === 'Amortização' ? 1 : 2;
}

export function dividendTypeFromCode(code: number): DividendType | null {
  if (code === 1) return 'Amortização';
  if (code === 2) return 'Dividendos';
  return null;
}

