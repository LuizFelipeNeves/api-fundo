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

export type BotCommand =
  | { kind: 'help' }
  | { kind: 'list' }
  | { kind: 'set'; codes: string[] }
  | { kind: 'add'; codes: string[] }
  | { kind: 'remove'; codes: string[] };

export function parseBotCommand(text: string): BotCommand {
  const raw = normalizeText(text);
  if (!raw) return { kind: 'help' };
  const lowered = raw.toLowerCase();
  const withoutSlash = lowered.startsWith('/') ? lowered.slice(1) : lowered;

  if (!withoutSlash || withoutSlash === 'start' || withoutSlash === 'help' || withoutSlash === 'ajuda' || withoutSlash === 'menu')
    return { kind: 'help' };
  if (withoutSlash === 'lista' || withoutSlash === 'minha lista') return { kind: 'list' };

  const setMatch = raw.match(/^(\/)?atualizar\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?set\s*:?\s*(.+)$/i);
  if (setMatch?.[2]) return { kind: 'set', codes: extractFundCodes(setMatch[2]) };

  const addMatch = raw.match(/^(\/)?adicionar\s+na\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?add\s*:?\s*(.+)$/i);
  if (addMatch?.[2]) return { kind: 'add', codes: extractFundCodes(addMatch[2]) };

  const rmMatch = raw.match(/^(\/)?remover\s+da\s+lista\s*:?\s*(.+)$/i) || raw.match(/^(\/)?remove\s*:?\s*(.+)$/i);
  if (rmMatch?.[2]) return { kind: 'remove', codes: extractFundCodes(rmMatch[2]) };

  const rawCodes = extractFundCodes(raw);
  if (rawCodes.length) return { kind: 'set', codes: rawCodes };

  return { kind: 'help' };
}

export function formatHelp(): string {
  return [
    'Comandos:',
    '- /menu',
    '- /lista',
    '- /set A, B, C',
    '- /add A',
    '- /remove B',
    '',
    'Você também pode mandar só a lista: HGLG11, MXRF11',
    '',
    'Exemplo: /set HGLG11, MXRF11',
  ].join('\n');
}
