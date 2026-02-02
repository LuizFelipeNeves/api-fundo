import type { ClockDeps, FetcherDeps, RepoDeps } from './types';
import { canonicalizeCotationsToday } from '../../parsers/today';

export async function syncFundCotationsToday<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchCotationsToday'>; repo: RepoDeps<Db>; clock: ClockDeps }
): Promise<{ status: 'ok'; code: string; changed: boolean }> {
  const raw = await deps.fetcher.fetchCotationsToday(code);
  const data = canonicalizeCotationsToday(raw);
  const fetchedAt = deps.clock.nowIso();
  const dataHash = deps.clock.sha256(JSON.stringify(data));
  const changed = deps.repo.upsertCotationsTodaySnapshot(db, code, fetchedAt, dataHash, data);
  return { status: 'ok', code: code.toUpperCase(), changed };
}
