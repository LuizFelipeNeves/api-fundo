import type Database from 'better-sqlite3';
import { getCotations, getDividends, getFundDetails, getLatestCotationsToday, getLatestIndicatorsSnapshots } from '../db/repo';
import { buildFundExportJson } from './fund-export/export-service';

export function exportFundJson(
  db: Database.Database,
  code: string,
  opts?: { cotationsDays?: number; indicatorsSnapshotsLimit?: number }
) {
  const fundCode = String(code || '').trim().toUpperCase();
  const details = getFundDetails(db, fundCode);
  if (!details) return null;

  const cotationsDays = Number.isFinite(opts?.cotationsDays) && (opts?.cotationsDays as number) > 0 ? Math.min(Math.floor(opts!.cotationsDays!), 5000) : 1825;
  const cotations = getCotations(db, fundCode, cotationsDays);
  const snapshotsLimit =
    Number.isFinite(opts?.indicatorsSnapshotsLimit) && (opts?.indicatorsSnapshotsLimit as number) > 0
      ? Math.min(Math.floor(opts!.indicatorsSnapshotsLimit!), 5000)
      : 365;
  const indicatorSnapshots = getLatestIndicatorsSnapshots(db, fundCode, snapshotsLimit);
  const dividends = getDividends(db, fundCode) ?? [];
  const cotationsToday = getLatestCotationsToday(db, fundCode) ?? [];

  return buildFundExportJson({
    details,
    cotations,
    dividends,
    indicatorSnapshots,
    cotationsToday,
    cotationsDays,
  });
}
