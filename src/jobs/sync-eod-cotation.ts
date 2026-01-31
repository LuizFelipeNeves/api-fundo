import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger } from './utils';

let lastRunDateIso: string | null = null;

function getSaoPauloTimeInfo(now: Date): { dateIso: string; minutes: number; dateBr: string } {
  const parts = new Intl.DateTimeFormat('pt-BR', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);

  const get = (type: string) => parts.find((p) => p.type === type)?.value || '';
  const year = get('year');
  const month = get('month');
  const day = get('day');
  const hour = Number(get('hour'));
  const minute = Number(get('minute'));
  const dateIso = year && month && day ? `${year}-${month}-${day}` : '';
  const dateBr = day && month && year ? `${day}/${month}/${year}` : '';
  return { dateIso, minutes: hour * 60 + minute, dateBr };
}

function shouldRunEod(now: Date): { run: boolean; dateIso: string; dateBr: string } {
  const { dateIso, minutes, dateBr } = getSaoPauloTimeInfo(now);
  if (!dateIso) return { run: false, dateIso: '', dateBr: '' };
  const afterClose = minutes >= 18 * 60 + 30;
  if (!afterClose) return { run: false, dateIso, dateBr };
  if (lastRunDateIso === dateIso) return { run: false, dateIso, dateBr };
  return { run: true, dateIso, dateBr };
}

export async function syncEodCotation(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-eod-cotation');
  const now = new Date();
  const gate = shouldRunEod(now);
  if (!gate.run) {
    log.skipped('outside_window_or_already_ran');
    return { ran: false };
  }
  lastRunDateIso = gate.dateIso;

  const db = getDb();
  const codes = repo.listFundCodes(db);
  log.start({ candidates: codes.length, date_iso: gate.dateIso });

  let ok = 0;
  let errCount = 0;
  let totalMs = 0;
  let maxMs = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    log.progress(i + 1, codes.length, code);
    const startedAt = Date.now();
    let status: 'ok' | 'err' = 'ok';
    try {
      const series = repo.getLatestCotationsToday(db, code);
      const last = series && series.length > 0 ? series[series.length - 1] : null;
      const price = last?.price ?? null;
      if (!Number.isFinite(price)) {
        ok++;
        continue;
      }
      repo.upsertCotationBrl(db, code, gate.dateIso, gate.dateBr, price as number);
      ok++;
    } catch (err) {
      status = 'err';
      errCount++;
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`sync-eod-cotation:${code}:${message.replace(/\n/g, '\\n')}\n`);
      continue;
    } finally {
      const durationMs = Date.now() - startedAt;
      totalMs += durationMs;
      maxMs = Math.max(maxMs, durationMs);
      log.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
    }
  }

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, err: errCount, avg_ms: avgMs, max_ms: maxMs, date_iso: gate.dateIso });
  return { ran: true };
}
