export function isWeekdayInSaoPaulo(date: Date): boolean {
  const weekday = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Sao_Paulo', weekday: 'short' }).format(date);
  return weekday !== 'Sat' && weekday !== 'Sun';
}

export function getMinutesInSaoPaulo(date: Date): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Sao_Paulo',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);

  let hour = 0;
  let minute = 0;
  for (const p of parts) {
    if (p.type === 'hour') hour = Number.parseInt(p.value, 10);
    if (p.type === 'minute') minute = Number.parseInt(p.value, 10);
  }

  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return 0;
  return hour * 60 + minute;
}

export function shouldRunCotationsToday(): boolean {
  const forceRaw = String(process.env.FORCE_RUN_JOBS || process.env.FORCE_RUN || '').trim().toLowerCase();
  if (forceRaw && forceRaw !== '0' && forceRaw !== 'false' && forceRaw !== 'no') return true;
  const now = new Date();
  if (!isWeekdayInSaoPaulo(now)) return false;
  const minutes = getMinutesInSaoPaulo(now);
  return minutes >= 10 * 60 && minutes <= 18 * 60 + 30;
}

export function shouldRunEodCotation(): boolean {
  const forceRaw = String(process.env.FORCE_RUN_JOBS || process.env.FORCE_RUN || '').trim().toLowerCase();
  if (forceRaw && forceRaw !== '0' && forceRaw !== 'false' && forceRaw !== 'no') return true;
  const now = new Date();
  if (!isWeekdayInSaoPaulo(now)) return false;
  const minutes = getMinutesInSaoPaulo(now);
  return minutes >= 18 * 60 + 30;
}
