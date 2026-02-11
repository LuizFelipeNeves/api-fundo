import { getCotations, getDividends, getFundDetails, getLatestCotationsToday, getLatestIndicatorsSnapshots } from '../db/repo';
import { buildFundExportJson } from './fund-export/export-service';

export async function exportFundJson(
  code: string,
  opts?: { cotationsDays?: number; indicatorsSnapshotsLimit?: number }
) {
  const fundCode = String(code || '').trim().toUpperCase();
  const details = await getFundDetails(fundCode);
  if (!details) return null;

  const cotationsDays = Number.isFinite(opts?.cotationsDays) && (opts?.cotationsDays as number) > 0
    ? Math.min(Math.floor(opts!.cotationsDays!), 5000)
    : 1825;
  const cotations = await getCotations(fundCode, cotationsDays);

  const snapshotsLimit =
    Number.isFinite(opts?.indicatorsSnapshotsLimit) && (opts?.indicatorsSnapshotsLimit as number) > 0
      ? Math.min(Math.floor(opts!.indicatorsSnapshotsLimit!), 5000)
      : 365;
  const indicatorSnapshots = await getLatestIndicatorsSnapshots(fundCode, snapshotsLimit);

  const dividends = (await getDividends(fundCode)) ?? [];
  const cotationsToday = (await getLatestCotationsToday(fundCode)) ?? [];

  return buildFundExportJson({
    details,
    cotations,
    dividends,
    indicatorSnapshots,
    cotationsToday,
    cotationsDays,
  });
}
