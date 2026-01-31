import type { FetcherDeps, RepoDeps } from './types';

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

  let dividendsChanges = 0;
  if (hasNewDocument) {
    const details = await deps.fetcher.fetchFIIDetails(code);
    deps.repo.updateFundDetails(db, details);

    const dividends = await deps.fetcher.fetchDividends(code);
    dividendsChanges = deps.repo.upsertDividends(db, code, dividends);
  }

  return { status: 'ok', code: code.toUpperCase(), docsInserted: inserted, hasNewDocument, dividendsChanges };
}
