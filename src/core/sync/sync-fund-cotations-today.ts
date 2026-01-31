import type { ClockDeps, FetcherDeps, RepoDeps } from './types';

export async function syncFundCotationsToday<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchCotationsToday'>; repo: RepoDeps<Db>; clock: ClockDeps }
): Promise<{ status: 'ok'; code: string; changed: boolean }> {
  const data = await deps.fetcher.fetchCotationsToday(code);
  const fetchedAt = deps.clock.nowIso();
  const dataHash = deps.clock.sha256(JSON.stringify(data));
  const changed = deps.repo.upsertCotationsTodaySnapshot(db, code, fetchedAt, dataHash, data);
  return { status: 'ok', code: code.toUpperCase(), changed };
}

