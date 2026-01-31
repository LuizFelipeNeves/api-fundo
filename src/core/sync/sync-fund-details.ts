import type { FetcherDeps, RepoDeps } from './types';

export async function syncFundDetails<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchFIIDetails'>; repo: Pick<RepoDeps<Db>, 'updateFundDetails'> }
): Promise<{ status: 'ok'; code: string }> {
  const details = await deps.fetcher.fetchFIIDetails(code);
  deps.repo.updateFundDetails(db, details);
  return { status: 'ok', code: code.toUpperCase() };
}

