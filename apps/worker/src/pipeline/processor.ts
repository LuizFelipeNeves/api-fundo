import { getReadModelWriter } from '../projections';
import { getWriteDb } from '../db';
import { createWriteSide } from './write-side';
import type { PersistRequest } from './messages';
import { dividend, cotation } from '../db/schema';
import { inArray, sql as drizzleSql } from 'drizzle-orm';

type DividendItem = { fund_code: string; date_iso: string; payment: string; type: number; value: number; yield: number };

async function enrichDividendYields(items: DividendItem[]) {
  if (items.length === 0) return items;

  const keys = items.map((i) => ({
    fundCode: i.fund_code.toUpperCase(),
    dateIso: i.date_iso,
    type: i.type,
  }));

  const existingRows = await getWriteDb().select({
    fundCode: dividend.fundCode,
    dateIso: dividend.dateIso,
    type: dividend.type,
    yield: dividend.yield,
  })
    .from(dividend)
    .where(
      inArray(
        drizzleSql`(${dividend.fundCode}, ${dividend.dateIso}, ${dividend.type})`,
        keys.map((k) => [k.fundCode, k.dateIso, k.type] as const)
      )
    );

  const existingMap = new Map<string, number>();
  for (const row of existingRows) {
    existingMap.set(`${row.fundCode}:${row.dateIso}:${row.type}`, row.yield ?? 0);
  }

  const needsPrice = items.filter((i) => {
    const key = `${i.fund_code.toUpperCase()}:${i.date_iso}:${i.type}`;
    if (existingMap.has(key)) return false;
    return !Number.isFinite(i.yield) || i.yield <= 0;
  });

  let priceMap = new Map<string, number>();
  if (needsPrice.length) {
    const priceKeys = needsPrice.map((i) => ({
      fundCode: i.fund_code.toUpperCase(),
      dateIso: i.date_iso,
    }));
    const priceRows = await getWriteDb().select({
      fundCode: cotation.fundCode,
      dateIso: cotation.dateIso,
      price: cotation.price,
    })
      .from(cotation)
      .where(
        inArray(
          drizzleSql`(${cotation.fundCode}, ${cotation.dateIso})`,
          priceKeys.map((k) => [k.fundCode, k.dateIso] as const)
        )
      );
    for (const r of priceRows) {
      if (r.price !== null) {
        priceMap.set(`${r.fundCode}:${r.dateIso}`, r.price);
      }
    }
  }

  return items.map((item) => {
    const key = `${item.fund_code.toUpperCase()}:${item.date_iso}:${item.type}`;
    const existingYield = existingMap.get(key);
    if (existingYield !== undefined) {
      return { ...item, yield: existingYield };
    }

    if (!Number.isFinite(item.yield) || item.yield <= 0) {
      const price = priceMap.get(`${item.fund_code.toUpperCase()}:${item.date_iso}`);
      if (Number.isFinite(price) && (price as number) > 0) {
        return { ...item, yield: item.value / (price as number) };
      }
    }

    return item;
  });
}

export async function processPersistRequest(request: PersistRequest): Promise<void> {
  const writeSide = createWriteSide();
  const readSide = getReadModelWriter();

  switch (request.type) {
    case 'fund_list': {
      await writeSide.upsertFundList(request.items);
      await readSide.upsertFundList(request.items);
      break;
    }
    case 'fund_details': {
      await writeSide.upsertFundDetails(request.item);
      await readSide.upsertFundDetails(request.item);
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
      await writeSide.upsertCotations(request.items);
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
    case 'dividends': {
      const enriched = await enrichDividendYields(request.items);
      await writeSide.upsertDividends(enriched);
      await readSide.upsertDividends(enriched);
      break;
    }
    case 'documents': {
      await writeSide.upsertDocuments(request.items);
      await readSide.upsertDocuments(request.items);
      break;
    }
    default: {
      const _exhaustive: never = request;
      throw new Error(`Unsupported persist type: ${String(_exhaustive)}`);
    }
  }
}
