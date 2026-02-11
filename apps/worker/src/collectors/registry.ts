import type { Collector } from './types';
import { fundListCollector } from './internal/fund-list';
import { fundDetailsCollector } from './internal/fund-details';
import { indicatorsCollector } from './internal/indicators';
import { cotationsCollector } from './internal/cotations';
import { cotationsTodayCollector } from './internal/cotations-today';
import { documentsCollector } from './internal/documents';

const collectors: Collector[] = [
  fundListCollector,
  fundDetailsCollector,
  indicatorsCollector,
  cotationsCollector,
  cotationsTodayCollector,
  documentsCollector,
];

export function getCollectors(): Collector[] {
  return collectors;
}

export function findCollector(name: string): Collector | null {
  const key = String(name || '').trim().toLowerCase();
  if (!key) return null;
  return collectors.find((c) => c.name === key) ?? null;
}
