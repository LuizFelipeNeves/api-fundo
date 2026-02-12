import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchDocuments } from '../../services/client';
import { toDateIsoFromBr } from '../../utils/date';

export const documentsCollector: Collector = {
  name: 'documents',
  supports(request: CollectRequest) {
    return request.collector === 'documents' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const cnpj = request.cnpj;

    if (!cnpj) {
      throw new Error('CNPJ is required for documents collector');
    }

    const data = await fetchDocuments(cnpj);

    const items = data.map((d) => ({
      fund_code: code,
      document_id: d.id,
      title: d.title,
      category: d.category,
      type: d.type,
      date: d.date,
      date_upload_iso: toDateIsoFromBr(d.dateUpload) || toDateIsoFromBr(d.date) || new Date().toISOString().slice(0, 10),
      dateUpload: d.dateUpload,
      url: d.url,
      status: d.status,
      version: d.version,
    }));

    const fetchedAt = new Date().toISOString();
    return {
      collector: 'documents',
      fetched_at: fetchedAt,
      payload: {
        persist_request: { type: 'documents', items, fund_code: code, fetched_at: fetchedAt },
      },
    };
  },
};
