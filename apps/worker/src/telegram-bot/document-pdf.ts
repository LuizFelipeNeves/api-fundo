import { createHash, randomUUID } from 'node:crypto';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function looksLikePdf(contentType: string | null, buf: Buffer): boolean {
  const ct = String(contentType || '').toLowerCase();
  if (ct.includes('application/pdf')) return true;
  if (buf.length >= 4 && buf.subarray(0, 4).toString('utf8') === '%PDF') return true;
  return false;
}

function isLikelyHtml(contentType: string | null, buf: Buffer): boolean {
  const ct = String(contentType || '').toLowerCase();
  if (ct.includes('text/html') || ct.includes('application/xhtml')) return true;
  const head = buf.subarray(0, 200).toString('utf8').toLowerCase();
  return head.includes('<html') || head.includes('<!doctype html') || head.includes('<head');
}

function maybeDecodeBase64Payload(buf: Buffer): Buffer {
  if (!buf || buf.length < 32) return buf;
  const raw = buf.toString('utf8').trim();
  if (!raw) return buf;
  const unquoted =
    raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"') ? raw.slice(1, -1).trim() : raw;
  if (!unquoted) return buf;
  if (unquoted.length < 64) return buf;
  if (!/^[A-Za-z0-9+/=\r\n]+$/.test(unquoted)) return buf;
  try {
    const decoded = Buffer.from(unquoted.replace(/\s+/g, ''), 'base64');
    if (!decoded || decoded.length < 32) return buf;
    if (looksLikePdf(null, decoded) || isLikelyHtml(null, decoded)) return decoded;
    return buf;
  } catch {
    return buf;
  }
}

async function writeTempFile(buf: Buffer, ext: string): Promise<string> {
  const dir = path.join(os.tmpdir(), 'api-fundo', 'telegram', 'documents');
  await fs.mkdir(dir, { recursive: true });
  const name = `${randomUUID()}-${createHash('sha1').update(buf).digest('hex').slice(0, 8)}${ext}`;
  const filePath = path.join(dir, name);
  await fs.writeFile(filePath, buf);
  return filePath;
}

function pickCandidateUrlsFromHtml(html: string, baseUrl: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  const push = (raw: string) => {
    const v = String(raw || '').trim();
    if (!v) return;
    try {
      const resolved = new URL(v, baseUrl).toString();
      if (seen.has(resolved)) return;
      seen.add(resolved);
      out.push(resolved);
    } catch {
      null;
    }
  };

  const patterns: RegExp[] = [
    /href\s*=\s*["']([^"']*downloadDocumento[^"']*)["']/gi,
    /["'](\/[^"']*downloadDocumento\?id=\d+[^"']*)["']/gi,
    /downloadDocumento\?id=(\d+)/gi,
    /downloadDocumento\?idDocumento=(\d+)/gi,
  ];

  for (const re of patterns) {
    re.lastIndex = 0;
    let m: RegExpExecArray | null = null;
    while ((m = re.exec(html))) {
      const value = m[1] ?? '';
      if (value && /^\d+$/.test(value)) {
        try {
          const origin = new URL(baseUrl).origin;
          push(new URL(`/fnet/publico/downloadDocumento?id=${value}`, origin).toString());
        } catch {
          null;
        }
      } else {
        push(value);
      }
      if (out.length >= 10) return out;
    }
  }

  return out;
}

export type DownloadedDocumentTempFile =
  | { kind: 'pdf'; filePath: string; url: string; contentType: string | null }
  | { kind: 'html'; filePath: string; url: string; contentType: string | null };

async function fileExists(filePath: string): Promise<boolean> {
  try {
    const st = await fs.stat(filePath);
    return st.isFile();
  } catch {
    return false;
  }
}

export async function downloadDocumentToDataFileCached(opts: {
  url: string;
  cacheDir: string;
  cacheKey: string;
}): Promise<DownloadedDocumentTempFile> {
  await fs.mkdir(opts.cacheDir, { recursive: true });
  const pdfPath = path.join(opts.cacheDir, `${opts.cacheKey}.pdf`);
  const htmlPath = path.join(opts.cacheDir, `${opts.cacheKey}.html`);

  if (await fileExists(pdfPath)) return { kind: 'pdf', filePath: pdfPath, url: opts.url, contentType: 'application/pdf' };
  if (await fileExists(htmlPath)) return { kind: 'html', filePath: htmlPath, url: opts.url, contentType: 'text/html' };

  const res = await fetch(opts.url);
  if (!res.ok) throw new Error(`DOWNLOAD_FAILED:${res.status}`);
  const contentType = res.headers.get('content-type');
  const buf = maybeDecodeBase64Payload(Buffer.from(await res.arrayBuffer()));
  if (looksLikePdf(contentType, buf)) {
    await fs.writeFile(pdfPath, buf);
    return { kind: 'pdf', filePath: pdfPath, url: res.url || opts.url, contentType };
  }

  if (!isLikelyHtml(contentType, buf)) throw new Error('DOCUMENT_NOT_PDF');

  const html = buf.toString('utf8');
  const baseUrl = res.url || opts.url;
  const candidates = pickCandidateUrlsFromHtml(html, baseUrl);
  for (const candidate of candidates) {
    try {
      const r2 = await fetch(candidate);
      if (!r2.ok) continue;
      const b2 = Buffer.from(await r2.arrayBuffer());
      if (!looksLikePdf(r2.headers.get('content-type'), b2)) continue;
      await fs.writeFile(pdfPath, b2);
      return { kind: 'pdf', filePath: pdfPath, url: r2.url || candidate, contentType: r2.headers.get('content-type') };
    } catch {
      null;
    }
  }

  await fs.writeFile(htmlPath, buf);
  return { kind: 'html', filePath: htmlPath, url: baseUrl, contentType };
}

export async function downloadDocumentToTempFile(url: string): Promise<DownloadedDocumentTempFile> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`DOWNLOAD_FAILED:${res.status}`);
  const contentType = res.headers.get('content-type');
  const buf = maybeDecodeBase64Payload(Buffer.from(await res.arrayBuffer()));
  if (looksLikePdf(contentType, buf)) {
    return { kind: 'pdf', filePath: await writeTempFile(buf, '.pdf'), url: res.url || url, contentType };
  }

  if (!isLikelyHtml(contentType, buf)) throw new Error('DOCUMENT_NOT_PDF');

  const html = buf.toString('utf8');
  const baseUrl = res.url || url;
  const candidates = pickCandidateUrlsFromHtml(html, baseUrl);
  for (const candidate of candidates) {
    try {
      const r2 = await fetch(candidate);
      if (!r2.ok) continue;
      const b2 = Buffer.from(await r2.arrayBuffer());
      if (!looksLikePdf(r2.headers.get('content-type'), b2)) continue;
      return { kind: 'pdf', filePath: await writeTempFile(b2, '.pdf'), url: r2.url || candidate, contentType: r2.headers.get('content-type') };
    } catch {
      null;
    }
  }

  return { kind: 'html', filePath: await writeTempFile(buf, '.html'), url: baseUrl, contentType };
}
