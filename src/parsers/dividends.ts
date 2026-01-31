export interface DividendData {
  value: number;
  yield: number;
  date: string;
  payment: string;
  type: 'Dividendos' | 'Amortização';
}

// Normaliza para 2 casas decimais
function normalizeDecimal(value: number): number {
  return Math.round(value * 100) / 100;
}

// Normaliza data para MM/yyyy
function toMonthKey(dateStr: string): string {
  const [, month, year] = dateStr.split('/');
  return `${month}/${year}`;
}

export function normalizeDividends(
  dividendsHtml: any[],
  dividendYield: any[]
): DividendData[] {
  // Criar map: MM/yyyy -> dividendValue (do HTML)
  const dividendValueMap = new Map<string, number>();
  for (const item of dividendsHtml) {
    if (item.type === 'Dividendos') {
      dividendValueMap.set(toMonthKey(item.date), item.value);
    }
  }

  // Criar map: MM/yyyy -> yield (do API)
  const yieldMap = new Map<string, number>();
  for (const item of dividendYield) {
    yieldMap.set(item.created_at, item.price);
  }

  const result: DividendData[] = dividendsHtml.map((item) => {
    const monthKey = toMonthKey(item.date);
    const yieldPercent = yieldMap.get(monthKey) ?? 0;
    const dividendValue = dividendValueMap.get(monthKey) ?? 0;

    if (item.type === 'Dividendos') {
      return {
        value: normalizeDecimal(item.value),
        yield: normalizeDecimal(yieldPercent),
        date: item.date,
        payment: item.payment,
        type: item.type,
      };
    } else {
      // Para amortização, calcular percentural de retorno usando o valor do dividendos do mesmo mês
      // valor_cota = (valor_dividendo / yield) * 100
      // capitalReturnPercent = (valor_amortizacao / valor_cota) * 100
      let capitalReturnPercent = 0;
      if (dividendValue > 0 && yieldPercent > 0) {
        const sharePrice = (dividendValue / yieldPercent) * 100;
        capitalReturnPercent = (item.value / sharePrice) * 100;
      }

      return {
        value: normalizeDecimal(item.value),
        yield: normalizeDecimal(capitalReturnPercent),
        date: item.date,
        payment: item.payment,
        type: item.type,
      };
    }
  });

  // Ordenar por data decrescente
  result.sort((a, b) => {
    const dateA = parseDate(a.date);
    const dateB = parseDate(b.date);
    return dateB.getTime() - dateA.getTime();
  });

  return result;
}

function parseDate(dateStr: string): Date {
  const [day, month, year] = dateStr.split('/').map(Number);
  return new Date(year, month - 1, day);
}
