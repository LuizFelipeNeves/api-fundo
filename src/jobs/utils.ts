export function pickCodesForRun(allCodes: string[], limit: number, bucketSeconds = 15 * 60): string[] {
  const effectiveLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
  if (effectiveLimit <= 0) return [];
  if (allCodes.length <= effectiveLimit) return allCodes;

  const bucket = Math.floor(Date.now() / (bucketSeconds * 1000));
  const start = bucket % allCodes.length;

  const out: string[] = [];
  for (let i = 0; i < effectiveLimit; i++) {
    out.push(allCodes[(start + i) % allCodes.length]);
  }
  return out;
}
