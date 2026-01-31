import type { FetcherDeps, RepoDeps } from './types';
import { syncFundDetailsAndDividends } from './sync-fund-details';

export async function syncFundDocuments<Db>(
  db: Db,
  code: string,
  deps: { fetcher: Pick<FetcherDeps, 'fetchDocuments' | 'fetchFIIDetails' | 'fetchDividends'>; repo: RepoDeps<Db> }
): Promise<
  | { status: 'skipped'; reason: 'missing_cnpj' }
  | { status: 'ok'; code: string; docsInserted: number; hasNewDocument: boolean; dividendsChanges: number }
> {
  const ref = deps.repo.getFundIdAndCnpj(db, code);
  const cnpj = ref?.cnpj ?? null;
  if (!cnpj) return { status: 'skipped', reason: 'missing_cnpj' };

  const state = deps.repo.getFundState(db, code);
  const lastMaxId = state?.last_documents_max_id ?? null;

  const docs = await deps.fetcher.fetchDocuments(cnpj);
  const { inserted, maxId } = deps.repo.upsertDocuments(db, code, docs);

  if (maxId) deps.repo.updateDocumentsMaxId(db, code, maxId);

  const hasNewDocument = maxId ? lastMaxId === null || maxId > lastMaxId : false;
  const dividendCount = deps.repo.getDividendCount(db, code);

  let dividendsChanges = 0;
  if (hasNewDocument || dividendCount === 0) {
    const result = await syncFundDetailsAndDividends(db, code, { fetcher: deps.fetcher, repo: deps.repo });
    dividendsChanges = result.dividendsChanges;
  }

  return { status: 'ok', code: code.toUpperCase(), docsInserted: inserted, hasNewDocument, dividendsChanges };
}
