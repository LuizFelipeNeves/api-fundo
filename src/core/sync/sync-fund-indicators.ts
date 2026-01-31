import type { ClockDeps, FetcherDeps, RepoDeps } from './types';

export async function syncFundIndicators<Db>(
  db: Db,
  code: string,
  deps: {
    fetcher: Pick<FetcherDeps, 'fetchFIIIndicators' | 'fetchFIICotations'>;
    repo: RepoDeps<Db>;
    clock: ClockDeps;
    options: { enableHistoricalBackfill: boolean; historicalDays: number };
  }
): Promise<
  | { status: 'skipped'; reason: 'missing_id' }
  | { status: 'ok'; code: string; indicatorsChanged: boolean; didHistoricalBackfill: boolean }
> {
  const ref = deps.repo.getFundIdAndCnpj(db, code);
  const id = ref?.id ?? null;
  if (!id) return { status: 'skipped', reason: 'missing_id' };

  const state = deps.repo.getFundState(db, code);
  const needsHistorical = deps.options.enableHistoricalBackfill && !state?.last_historical_cotations_at;

  let didHistoricalBackfill = false;
  if (needsHistorical) {
    const days = deps.options.historicalDays;
    const safeDays = Number.isFinite(days) && days > 0 ? Math.min(days, 1825) : 365;
    const cotations = await deps.fetcher.fetchFIICotations(id, safeDays);
    deps.repo.upsertCotationsHistoricalBrl(db, code, cotations);
    didHistoricalBackfill = true;
  }

  const indicators = await deps.fetcher.fetchFIIIndicators(id);
  const fetchedAt = deps.clock.nowIso();
  const dataHash = deps.clock.sha256(JSON.stringify(indicators));
  const indicatorsChanged = deps.repo.upsertIndicatorsSnapshot(db, code, fetchedAt, dataHash, indicators);

  return { status: 'ok', code: code.toUpperCase(), indicatorsChanged, didHistoricalBackfill };
}

