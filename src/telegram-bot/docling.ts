import { execFile } from 'node:child_process';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

function errToStderrTail(err: unknown, maxChars: number): string {
  const anyErr = err as any;
  const stderr = typeof anyErr?.stderr === 'string' ? anyErr.stderr : '';
  if (!stderr) return '';
  const v = stderr.trimEnd();
  if (v.length <= maxChars) return v;
  return v.slice(Math.max(0, v.length - maxChars));
}

async function pickBestTextFileFromDir(dirPath: string): Promise<string | null> {
  let dirents: Array<{ name: string; isFile: boolean }> = [];
  try {
    const d = await fs.readdir(dirPath, { withFileTypes: true });
    dirents = d.map((x) => ({ name: x.name, isFile: x.isFile() }));
  } catch {
    return null;
  }

  const files = dirents.filter((e) => e.isFile).map((e) => path.join(dirPath, e.name));
  if (files.length === 0) return null;

  const preferredExts = ['.txt', '.md', '.markdown'];

  let best: { fp: string; rank: number; size: number } | null = null;
  for (const fp of files) {
    const ext = path.extname(fp).toLowerCase();
    const idx = preferredExts.indexOf(ext);
    const rank = idx === -1 ? preferredExts.length : idx;
    try {
      const st = await fs.stat(fp);
      if (!best) {
        best = { fp, rank, size: st.size };
        continue;
      }
      if (rank < best.rank) best = { fp, rank, size: st.size };
      else if (rank === best.rank && st.size > best.size) best = { fp, rank, size: st.size };
    } catch {
      null;
    }
  }

  return best?.fp ?? null;
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    const st = await fs.stat(filePath);
    return st.isFile();
  } catch {
    return false;
  }
}

export async function doclingConvertToText(inputPath: string, outputDir: string): Promise<string> {
  const extractedPath = path.join(outputDir, 'extracted.txt');
  await fs.mkdir(outputDir, { recursive: true });

  if (await fileExists(extractedPath)) return extractedPath;

  let doclingErr: unknown | null = null;
  try {
    await execFileAsync('docling', [inputPath, '--output', outputDir, '--to', 'text'], { timeout: 240_000, maxBuffer: 80 * 1024 * 1024 });
  } catch (err) {
    doclingErr = err;
  }

  const bestTextFile = await pickBestTextFileFromDir(outputDir);
  if (bestTextFile) {
    const extracted = await fs.readFile(bestTextFile, 'utf8');
    await fs.writeFile(extractedPath, extracted.trimEnd() ? `${extracted.trimEnd()}\n` : '', 'utf8');
    return extractedPath;
  }

  const tail = errToStderrTail(doclingErr, 4000);
  if (tail) throw new Error(`DOCLING_FAILED:${tail}`);
  throw new Error('DOCLING_FAILED');
}
