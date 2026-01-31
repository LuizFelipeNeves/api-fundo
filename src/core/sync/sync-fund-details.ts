import type { FetcherDeps, RepoDeps } from './types';

export async function syncFundDetails<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchFIIDetails'>; repo: Pick<RepoDeps<Db>, 'updateFundDetails'> }
): Promise<{ status: 'ok'; code: string }> {
  const { details } = await deps.fetcher.fetchFIIDetails(code);
  deps.repo.updateFundDetails(db, details);
  return { status: 'ok', code: code.toUpperCase() };
}

export async function syncFundDetailsAndDividends<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchFIIDetails' | 'fetchDividends'>; repo: Pick<RepoDeps<Db>, 'updateFundDetails' | 'upsertDividends'> }
): Promise<{ status: 'ok'; code: string; dividendsChanges: number }> {
  const { details, dividendsHistory } = await deps.fetcher.fetchFIIDetails(code);
  deps.repo.updateFundDetails(db, details);

  const dividends = await deps.fetcher.fetchDividends(code, { id: details.id, dividendsHistory });
  const dividendsChanges = deps.repo.upsertDividends(db, code, dividends);

  return { status: 'ok', code: code.toUpperCase(), dividendsChanges };
}
