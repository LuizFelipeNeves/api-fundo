function normalizeText(text: string): string {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractFundCodes(raw: string): string[] {
  const upper = String(raw || '').toUpperCase();
  const matches = upper.match(/[A-Z]{4}\d{2}/g) ?? [];
  return Array.from(new Set(matches));
}

function extractFirstInt(raw: string): number | null {
  const m = String(raw || '').match(/\b(\d{1,4})\b/);
  if (!m) return null;
  const n = Number(m[1]);
  if (!Number.isFinite(n)) return null;
  return n;
}

export type BotCommand =
  | { kind: 'help' }
  | { kind: 'list' }
  | { kind: 'categories' }
  | { kind: 'confirm' }
  | { kind: 'cancel' }
  | { kind: 'set'; codes: string[] }
  | { kind: 'add'; codes: string[] }
  | { kind: 'remove'; codes: string[] }
  | { kind: 'documentos'; code?: string; limit?: number }
  | { kind: 'pesquisa'; code: string }
  | { kind: 'cotation'; code: string };

export function parseBotCommand(text: string): BotCommand {
  const raw = normalizeText(text);
  if (!raw) return { kind: 'help' };
  const lowered = raw.toLowerCase();
  const withoutSlash = lowered.startsWith('/') ? lowered.slice(1) : lowered;
  const withoutMention = withoutSlash.replace(/@[\w_]+/g, '').trim();
  const firstWord = withoutMention.split(' ')[0] || '';

  if (!withoutSlash || withoutSlash === 'start' || withoutSlash === 'help' || withoutSlash === 'ajuda' || withoutSlash === 'menu')
    return { kind: 'help' };
  if (withoutSlash === 'lista' || withoutSlash === 'minha lista') return { kind: 'list' };
  if (withoutSlash === 'categorias' || withoutSlash === 'categoria') return { kind: 'categories' };
  if (withoutMention === 'confirm' || withoutMention === 'confirmar' || withoutMention === 'sim') return { kind: 'confirm' };
  if (withoutMention === 'cancel' || withoutMention === 'cancelar' || withoutMention === 'nao' || withoutMention === 'não')
    return { kind: 'cancel' };

  const documentosMatch = raw.match(/^(\/)?documentos\b(.*)$/i);
  if (documentosMatch) {
    const rest = normalizeText(documentosMatch[2] || '');
    const limit = extractFirstInt(rest);
    const codes = extractFundCodes(rest);
    const code = codes[0];
    return { kind: 'documentos', code, limit: limit === null ? undefined : limit };
  }

  const pesquisaMatch = raw.match(/^(\/)?pesquisa\b(.*)$/i);
  if (pesquisaMatch) {
    const rest = normalizeText(pesquisaMatch[2] || '');
    const code = extractFundCodes(rest)[0];
    if (code) return { kind: 'pesquisa', code };
    return { kind: 'help' };
  }

  const cotationMatch = raw.match(/^(\/)?(cotation|contation|cotacao)\b(.*)$/i);
  if (cotationMatch) {
    const rest = normalizeText(cotationMatch[3] || '');
    const code = extractFundCodes(rest)[0];
    if (code) return { kind: 'cotation', code };
    return { kind: 'help' };
  }

  const setMatch = raw.match(/^(\/)?atualizar\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?set\s*:?\s*(.+)$/i);
  if (setMatch?.[2]) return { kind: 'set', codes: extractFundCodes(setMatch[2]) };

  const addMatch = raw.match(/^(\/)?adicionar\s+na\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?add\s*:?\s*(.+)$/i);
  if (addMatch?.[2]) return { kind: 'add', codes: extractFundCodes(addMatch[2]) };

  const rmMatch = raw.match(/^(\/)?remover\s+da\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?remove\s*:?\s*(.+)$/i);
  if (rmMatch?.[2]) return { kind: 'remove', codes: extractFundCodes(rmMatch[2]) };

  if (
    lowered.startsWith('/') &&
    firstWord &&
    ![
      'start',
      'help',
      'ajuda',
      'menu',
      'lista',
      'minha',
      'categorias',
      'categoria',
      'documentos',
      'pesquisa',
      'cotation',
      'contation',
      'cotacao',
      'set',
      'atualizar',
      'add',
      'adicionar',
      'remove',
      'remover',
    ].includes(firstWord)
  ) {
    return { kind: 'help' };
  }

  const rawCodes = extractFundCodes(raw);
  if (rawCodes.length) return { kind: 'set', codes: rawCodes };

  return { kind: 'help' };
}

export function formatHelp(): string {
  return [
    'Comandos:',
    '- /menu',
    '- /lista',
    '- /categorias',
    '- /documentos [FUNDO] [N]',
    '- /pesquisa FUNDO',
    '- /cotation FUNDO',
    '- /set A, B, C',
    '- /add A',
    '- /remove B',
    '',
    'Você também pode mandar só a lista: HGLG11, MXRF11',
    '',
    'Exemplo: /set HGLG11, MXRF11',
    'Exemplo: /documentos (padrão 5)',
    'Exemplo: /documentos 10',
    'Exemplo: /documentos HGLG11',
    'Exemplo: /documentos HGLG11 5',
    'Exemplo: /pesquisa HGLG11',
    'Exemplo: /cotation HGLG11',
  ].join('\n');
}
