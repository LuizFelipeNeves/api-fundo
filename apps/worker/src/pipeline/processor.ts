import { createWriteSide } from './write-side';
import type { PersistRequest } from './messages';
import { enrichDividendYields } from '../utils/dividends';

const writeSide = createWriteSide();

export async function processPersistRequest(request: PersistRequest): Promise<void> {
  switch (request.type) {
    case 'fund_list': {
      await writeSide.upsertFundList(request.items);
      break;
    }
    case 'fund_details': {
      await writeSide.upsertFundDetails(request.item);
      if (request.dividends && request.dividends.length > 0) {
        const enriched = await enrichDividendYields(request.dividends);
        await writeSide.upsertDividends(enriched);
      }
      break;
    }
    case 'indicators': {
      await writeSide.upsertIndicators(request.item);
      break;
    }
    case 'cotations': {
      await writeSide.upsertCotations(request.items, {
        fund_code: request.fund_code,
        fetched_at: request.fetched_at,
        is_last_chunk: request.is_last_chunk,
      });
      break;
    }
    case 'cotations_today': {
      await writeSide.upsertCotationsToday(request.item);
      break;
    }
    case 'documents': {
      await writeSide.upsertDocuments(request.items, { fund_code: request.fund_code, fetched_at: request.fetched_at });
      break;
    }
    default: {
      const _exhaustive: never = request;
      throw new Error(`Unsupported persist type: ${String(_exhaustive)}`);
    }
  }
}
