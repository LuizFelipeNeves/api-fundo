import { getReadModelWriter } from '../projections';
import { createWriteSide } from './write-side';
import type { PersistRequest } from './messages';
import { enrichDividendYields } from '../utils/dividends';

const writeSide = createWriteSide();
const readSide = getReadModelWriter();

export async function processPersistRequest(request: PersistRequest): Promise<void> {
  switch (request.type) {
    case 'fund_list': {
      await writeSide.upsertFundList(request.items);
      await readSide.upsertFundList(request.items);
      break;
    }
    case 'fund_details': {
      await writeSide.upsertFundDetails(request.item);
      await readSide.upsertFundDetails(request.item);
      if (request.dividends && request.dividends.length > 0) {
        const enriched = await enrichDividendYields(request.dividends);
        await writeSide.upsertDividends(enriched);
        await readSide.upsertDividends(enriched);
      }
      break;
    }
    case 'indicators': {
      const { fund_code, fetched_at, data_json } = request.item;
      await writeSide.upsertIndicators(request.item);
      const fetchedAtDate = new Date(fetched_at);
      await readSide.upsertIndicatorsLatest(fund_code, fetchedAtDate, data_json);
      await readSide.insertIndicatorsSnapshot(fund_code, fetchedAtDate, data_json);
      break;
    }
    case 'cotations': {
      await writeSide.upsertCotations(request.items, {
        fund_code: request.fund_code,
        fetched_at: request.fetched_at,
        is_last_chunk: request.is_last_chunk,
      });
      await readSide.upsertCotations(request.items);
      break;
    }
    case 'cotations_today': {
      await writeSide.upsertCotationsToday(request.item);
      await readSide.insertCotationsToday(
        request.item.fund_code,
        request.item.date_iso,
        new Date(request.item.fetched_at),
        request.item.data_json
      );
      break;
    }
    case 'documents': {
      await writeSide.upsertDocuments(request.items, { fund_code: request.fund_code, fetched_at: request.fetched_at });
      await readSide.upsertDocuments(request.items);
      break;
    }
    default: {
      const _exhaustive: never = request;
      throw new Error(`Unsupported persist type: ${String(_exhaustive)}`);
    }
  }
}
