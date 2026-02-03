import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { exportFundJson } from '../../services/fund-export';
import { listExistingFundCodes, listTelegramUserFunds } from '../storage';
import type { HandlerDeps } from './types';

export async function handleExport({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const requested = codes.length ? codes.map((c) => c.toUpperCase()) : listTelegramUserFunds(db, chatIdStr);
  if (!requested.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }

  const existing = listExistingFundCodes(db, requested);
  const missing = requested.filter((code) => !existing.includes(code));

  const funds: Array<{ code: string; ok: boolean; data?: any; error?: string }> = [];
  for (const code of existing) {
    const data = exportFundJson(db, code);
    if (!data) {
      funds.push({ code, ok: false, error: 'FII não encontrado' });
    } else {
      funds.push({ code, ok: true, data });
    }
  }

  const payload = {
    generated_at: new Date().toISOString(),
    source: 'telegram',
    chat_id: chatIdStr,
    requested_codes: requested,
    exported_codes: existing,
    missing_codes: missing,
    funds,
  };

  const safeStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `fii-export-${chatIdStr}-${safeStamp}.json`;
  const filePath = path.join(os.tmpdir(), filename);
  await fs.writeFile(filePath, JSON.stringify(payload), 'utf8');
  try {
    const caption = `Export JSON: ${existing.length} fundo(s)${missing.length ? ` (${missing.length} não encontrado(s))` : ''}`;
    const res = await telegram.sendDocument(chatIdStr, { filePath, filename, caption, contentType: 'application/json' });
    if (!res.ok) {
      await telegram.sendText(chatIdStr, 'Não consegui enviar o arquivo agora. Tente novamente.');
    }
  } finally {
    await fs.unlink(filePath).catch(() => {});
  }
}
