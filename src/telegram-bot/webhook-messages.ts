import type { DocumentData } from '../parsers/documents';

function normalizeCategoryKey(value: string): string {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function cleanLine(value: unknown): string {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function formatDateHuman(value: unknown): string {
  const v = cleanLine(value);
  if (!v) return '';
  const iso = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[3]}/${iso[2]}/${iso[1]}`;
  const br = v.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (br) return v;
  const my = v.match(/^(\d{2})\/(\d{4})$/);
  if (my) return v;
  return v;
}

function pickCategoryEmoji(category: string): string {
  const key = normalizeCategoryKey(category);
  if (key.includes('titulo') || key.includes('valores mobiliarios')) return '📄';
  if (key.includes('fiagro')) return '🌾';
  if (key.includes('hibrid')) return '🏢';
  if (key.includes('infra')) return '⚙️';
  if (key.includes('logistic') || key.includes('industr') || key.includes('galp')) return '🏭';
  if (key.includes('shopping') || key.includes('varejo')) return '🛍️';
  if (key.includes('lajes') || key.includes('corporativ')) return '🏙️';
  if (key.includes('hospital')) return '🏥';
  if (key.includes('agencia') && key.includes('banc')) return '🏦';
  if (key.includes('educa')) return '🎓';
  if (key.includes('hote')) return '🏨';
  if (key.includes('residenc')) return '🏘️';
  if (key.includes('fundo de fundos') || key === 'fof') return '🧺';
  if (key.includes('fip') || key.includes('participacoes')) return '🤝';
  if (key.includes('tijolo')) return '🧱';
  if (key.includes('papel')) return '📄';
  if (key.includes('misto')) return '🏢';
  if (key.includes('desenvolvimento')) return '🏗️';
  if (key.includes('outro')) return '🧩';
  if (key.includes('sem categoria')) return '❓';
  if (key.includes('desconhecid')) return '❓';
  return '📌';
}

export type FundCategoryRow = {
  code: string;
  segmento: string | null;
  sector: string | null;
  tipo_fundo: string | null;
  type: string | null;
};

export function formatFundsListMessage(funds: string[]): string {
  if (!funds.length) return '📭 Sua lista está vazia.';
  return [`📌 Sua lista (${funds.length} fundos)`, '', funds.join(', ')].join('\n');
}

export function formatCategoriesMessage(funds: string[], info: FundCategoryRow[]): string {
  if (!funds.length) return 'Sua lista está vazia.';

  const byCode = new Map(
    info.map((r) => {
      const picked = String(r.segmento || r.sector || r.tipo_fundo || r.type || '').trim();
      return [r.code.toUpperCase(), picked || '(sem categoria)'] as const;
    })
  );

  const groups = new Map<string, string[]>();
  for (const code of funds) {
    const cat = byCode.get(code) ?? '(sem categoria)';
    const list = groups.get(cat);
    if (list) list.push(code);
    else groups.set(cat, [code]);
  }

  const sorted = Array.from(groups.entries()).sort((a, b) => {
    const byCount = b[1].length - a[1].length;
    if (byCount) return byCount;
    return a[0].localeCompare(b[0]);
  });

  const lines: string[] = [];
  for (const [cat, codes] of sorted) {
    const emoji = pickCategoryEmoji(cat);
    lines.push(`${emoji} ${cat} (${codes.length})`);
    const shown = codes.slice(0, 50);
    const suffix = codes.length > shown.length ? ', …' : '';
    lines.push(`${shown.join(', ')}${suffix}`);
    lines.push('');
  }

  return lines.join('\n').trim();
}

export function formatNewDocumentMessage(fundCode: string, d: DocumentData): string {
  const code = cleanLine(fundCode).toUpperCase();
  const id = Number.isFinite(d.id) ? String(d.id) : '';
  const title = cleanLine(d.title);
  const category = cleanLine(d.category);
  const type = cleanLine(d.type);
  const status = cleanLine(d.status);
  const version = Number.isFinite(d.version) ? String(d.version) : '';
  const url = cleanLine(d.url);

  const docType = [category, type].filter(Boolean).join(' · ');

  const upload = formatDateHuman(d.dateUpload);
  const ref = formatDateHuman(d.date);
  const when = upload ? `🗓️ Upload: ${upload}${ref && ref !== upload ? ` (ref: ${ref})` : ''}` : ref ? `🗓️ Ref: ${ref}` : '';

  const header = `📰 Novo documento — ${code}`;
  const lines: string[] = [header];
  if (docType) lines.push(`🗂️ ${docType}`);
  if (title) lines.push(`📝 ${title}`);
  if (when) lines.push(when);
  if (status) lines.push(`📌 Status: ${status}`);
  if (version && version !== '1') lines.push(`🔢 Versão: ${version}`);
  if (id) lines.push(`🆔 ID: ${id}`);
  if (url) lines.push(`🔗 ${url}`);
  lines.push(`📚 Ver mais: /documentos ${code}`);
  return lines.join('\n').trim();
}

function clipTelegramText(value: string, maxChars: number): string {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= maxChars) return v;
  return `${v.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

export function formatResumoDocumentoMessage(opts: {
  fundCode: string;
  doc: { id: number; category: string; type: string; dateUpload: string; url: string };
  extractedText: string;
}): string {
  const code = cleanLine(opts.fundCode).toUpperCase();
  const docType = [cleanLine(opts.doc.category), cleanLine(opts.doc.type)].filter(Boolean).join(' · ');
  const upload = formatDateHuman(opts.doc.dateUpload);
  const id = Number.isFinite(opts.doc.id) ? String(opts.doc.id) : '';
  const url = cleanLine(opts.doc.url);

  const normalized = String(opts.extractedText || '')
    .replace(/\r/g, '')
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .join('\n');
  const snippet = clipTelegramText(normalized, 2800);

  const lines: string[] = [`🧾 Resumo do documento — ${code}`];
  if (docType) lines.push(`🗂️ ${docType}`);
  if (upload) lines.push(`🗓️ Upload: ${upload}`);
  if (id) lines.push(`🆔 ID: ${id}`);
  if (snippet) lines.push('', snippet);
  if (url) lines.push('', `🔗 ${url}`);
  return lines.join('\n').trim();
}

export function formatSetMessage(opts: {
  existing: string[];
  added: string[];
  removed: string[];
  missing: string[];
}): string {
  const lines: string[] = [];
  lines.push(opts.existing.length ? `✅ Lista atualizada (${opts.existing.length} fundos)` : '✅ Lista atualizada (vazia)');
  if (opts.existing.length) lines.push('', `📌 Fundos`, opts.existing.join(', '));
  if (opts.added.length) lines.push('', `➕ Adicionados`, opts.added.join(', '));
  if (opts.removed.length) lines.push('', `➖ Removidos`, opts.removed.join(', '));
  if (opts.missing.length) lines.push('', `❓ Não encontrei no banco`, opts.missing.join(', '));
  return lines.join('\n').trim();
}

export function formatAddMessage(opts: { addedCount: number; nowList: string[]; missing: string[] }): string {
  const lines: string[] = [`➕ Adicionados: ${opts.addedCount}`];
  lines.push(opts.nowList.length ? `📌 Agora (${opts.nowList.length} fundos)` : '📭 Agora: (vazia)');
  if (opts.nowList.length) lines.push(opts.nowList.join(', '));
  if (opts.missing.length) lines.push('', `❓ Não encontrei no banco`, opts.missing.join(', '));
  return lines.join('\n').trim();
}

export function formatRemoveMessage(opts: { removedCount: number; nowList: string[]; missing: string[] }): string {
  const lines: string[] = [`➖ Removidos: ${opts.removedCount}`];
  lines.push(opts.nowList.length ? `📌 Agora (${opts.nowList.length} fundos)` : '📭 Agora: (vazia)');
  if (opts.nowList.length) lines.push(opts.nowList.join(', '));
  if (opts.missing.length) lines.push('', `❓ Não encontrei no banco`, opts.missing.join(', '));
  return lines.join('\n').trim();
}

export type LatestDocumentRow = {
  fund_code: string;
  title: string;
  category: string;
  type: string;
  dateUpload: string;
  url: string;
};

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  const pct = value * 100;
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(2)}%`;
}

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toLocaleString('pt-BR', { maximumFractionDigits: digits });
}

function formatDateIso(value: string | null | undefined): string {
  const v = String(value || '').trim();
  const m = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return v || '—';
  return `${m[3]}/${m[2]}/${m[1]}`;
}

function formatTimeFromIso(value: string | null | undefined): string {
  const d = new Date(String(value || '').trim());
  if (!Number.isFinite(d.getTime())) return '';
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return `${hh}:${mm}`;
}

function formatDateTimeFromIso(value: string | null | undefined): string {
  const iso = String(value || '').trim();
  if (!iso) return '';
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return '';
  const date = d.toISOString().slice(0, 10);
  const time = formatTimeFromIso(iso);
  return `${formatDateIso(date)}${time ? ` ${time}` : ''}`.trim();
}

function isSameLocalDay(a: Date, b: Date): boolean {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function formatPrice(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatDocumentsMessage(opts: { docs: LatestDocumentRow[]; limit: number; code?: string }): string {
  if (!opts.docs.length) {
    return opts.code ? `📰 Não encontrei documentos para ${opts.code}.` : '📰 Não encontrei documentos para sua lista.';
  }

  const header = opts.code ? `📰 Documentos — ${opts.code}` : '📰 Documentos — sua lista';
  const sub = `Mostrando ${opts.docs.length} de ${opts.limit} (mais recentes)`;

  const lines: string[] = [header, sub, ''];
  for (const d of opts.docs) {
    const code = String(d.fund_code || '').toUpperCase();
    const title = String(d.title || '').trim();
    const date = String(d.dateUpload || '').trim();
    const docType = [String(d.category || '').trim(), String(d.type || '').trim()].filter(Boolean).join(' · ');
    const url = String(d.url || '').trim();
    lines.push(`📌 ${code}${date ? ` • ${date}` : ''}`);
    if (docType) lines.push(`🗂️ ${docType}`);
    if (title) lines.push(`📝 ${title}`);
    if (url) lines.push(`🔗 ${url}`);
    lines.push('');
  }
  return lines.join('\n').trim();
}

export function formatPesquisaMessage(opts: {
  fund: {
    code: string;
    sector?: string | null;
    type?: string | null;
    segmento?: string | null;
    tipo_fundo?: string | null;
    p_vp?: number | null;
    dividend_yield?: number | null;
    dividend_yield_last_5_years?: number | null;
    daily_liquidity?: number | null;
    net_worth?: number | null;
    razao_social?: string | null;
    cnpj?: string | null;
    publico_alvo?: string | null;
    mandato?: string | null;
    prazo_duracao?: string | null;
    tipo_gestao?: string | null;
    taxa_adminstracao?: string | null;
    vacancia?: number | null;
    numero_cotistas?: number | null;
    cotas_emitidas?: number | null;
    valor_patrimonial_cota?: number | null;
    valor_patrimonial?: number | null;
    ultimo_rendimento?: number | null;
    updated_at?: string | null;
  };
}): string {
  const f = opts.fund;
  const code = f.code.toUpperCase();

  const lines: string[] = [`🔎 Pesquisa — ${code}`];
  const name = String(f.razao_social || '').trim();
  if (name) lines.push(name);
  const cnpj = String(f.cnpj || '').trim();
  if (cnpj) lines.push(`🏷️ CNPJ: ${cnpj}`);

  const catParts = [
    String(f.segmento || '').trim(),
    String(f.tipo_fundo || '').trim(),
    String(f.sector || '').trim(),
    String(f.type || '').trim(),
  ].filter(Boolean);
  const seen = new Set<string>();
  const cat = catParts
    .filter((p) => {
      const key = normalizeCategoryKey(p);
      if (!key) return false;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .join(' · ');
  if (cat) lines.push(`📚 Categoria: ${cat}`);

  const line1: string[] = [];
  if (f.p_vp !== null && f.p_vp !== undefined) line1.push(`💰 P/VP ${formatNumber(f.p_vp, 2)}`);
  if (f.daily_liquidity !== null && f.daily_liquidity !== undefined) line1.push(`📊 Liquidez ${formatNumber(f.daily_liquidity, 0)}`);
  if (f.net_worth !== null && f.net_worth !== undefined) line1.push(`🏦 PL ${formatNumber(f.net_worth, 0)}`);
  if (line1.length) lines.push('', line1.join(' | '));

  const line2: string[] = [];
  if (f.dividend_yield !== null && f.dividend_yield !== undefined) line2.push(`💸 DY ${formatNumber(f.dividend_yield, 2)}`);
  if (f.dividend_yield_last_5_years !== null && f.dividend_yield_last_5_years !== undefined)
    line2.push(`📆 DY 5a ${formatNumber(f.dividend_yield_last_5_years, 2)}`);
  if (f.ultimo_rendimento !== null && f.ultimo_rendimento !== undefined) line2.push(`🧾 Últ. rend. ${formatNumber(f.ultimo_rendimento, 4)}`);
  if (line2.length) lines.push(line2.join(' | '));

  const line3: string[] = [];
  const vac = f.vacancia;
  if (vac !== null && vac !== undefined) line3.push(`🏚️ Vacância ${formatNumber(vac, 2)}`);
  if (f.numero_cotistas !== null && f.numero_cotistas !== undefined) line3.push(`👥 Cotistas ${formatNumber(f.numero_cotistas, 0)}`);
  if (f.cotas_emitidas !== null && f.cotas_emitidas !== undefined) line3.push(`🧩 Cotas ${formatNumber(f.cotas_emitidas, 0)}`);
  if (line3.length) lines.push(line3.join(' | '));

  const line4: string[] = [];
  if (f.valor_patrimonial_cota !== null && f.valor_patrimonial_cota !== undefined)
    line4.push(`🏷️ VP/cota ${formatNumber(f.valor_patrimonial_cota, 2)}`);
  if (f.valor_patrimonial !== null && f.valor_patrimonial !== undefined) line4.push(`🏛️ VP ${formatNumber(f.valor_patrimonial, 0)}`);
  if (line4.length) lines.push(line4.join(' | '));

  const extra = [
    String(f.publico_alvo || '').trim() ? `🎯 Público: ${String(f.publico_alvo || '').trim()}` : '',
    String(f.mandato || '').trim() ? `🧭 Mandato: ${String(f.mandato || '').trim()}` : '',
    String(f.tipo_gestao || '').trim() ? `🧑‍💼 Gestão: ${String(f.tipo_gestao || '').trim()}` : '',
    String(f.prazo_duracao || '').trim() ? `⏳ Prazo: ${String(f.prazo_duracao || '').trim()}` : '',
    String(f.taxa_adminstracao || '').trim() ? `🧾 Taxa adm.: ${String(f.taxa_adminstracao || '').trim()}` : '',
  ].filter(Boolean);
  if (extra.length) lines.push('', ...extra);

  return lines.join('\n').trim();
}

export function formatCotationMessage(opts: {
  fundCode: string;
  asOfDateIso: string;
  lastPrice: number;
  returns: { d7: number | null; d30: number | null; d90: number | null };
  drawdown: { max: number | null };
  volatility: { d30: number | null; d90: number | null };
  computedAt: string;
}): string {
  const code = opts.fundCode.toUpperCase();
  const lines: string[] = [
    `📈 Cotação — ${code}`,
    `🗓️ Data base: ${formatDateIso(opts.asOfDateIso)}`,
    `💰 Último preço: R$ ${formatPrice(opts.lastPrice)}`,
    '',
  ];

  lines.push(`📊 Variações`, `- 7d: ${formatPercent(opts.returns.d7)}`, `- 30d: ${formatPercent(opts.returns.d30)}`, `- 90d: ${formatPercent(opts.returns.d90)}`, '');

  lines.push(`📉 Drawdown máximo: ${formatPercent(opts.drawdown.max)}`);

  const v30 = opts.volatility.d30 === null ? '—' : `${(opts.volatility.d30 * 100).toFixed(2)}%`;
  const v90 = opts.volatility.d90 === null ? '—' : `${(opts.volatility.d90 * 100).toFixed(2)}%`;
  lines.push(`🌪️ Volatilidade (anualizada): 30d ${v30} | 90d ${v90}`);

  const now = new Date();
  const computed = new Date(String(opts.computedAt || '').trim());
  if (Number.isFinite(computed.getTime())) {
    const ageMs = now.getTime() - computed.getTime();
    if (!(ageMs >= 0 && ageMs <= 30_000)) {
      const stamp = isSameLocalDay(now, computed) ? formatTimeFromIso(opts.computedAt) : formatDateTimeFromIso(opts.computedAt);
      if (stamp) lines.push('', `🗄️ Cache: ${stamp}`);
    }
  }
  return lines.join('\n').trim();
}

export function formatConfirmSetMessage(opts: { beforeCount: number; afterCodes: string[]; added: string[]; removed: string[]; missing: string[] }): string {
  const lines: string[] = [];
  lines.push('⚠️ Confirmação necessária');
  lines.push(`Você está prestes a atualizar sua lista: ${opts.beforeCount} → ${opts.afterCodes.length} fundos`);
  if (opts.added.length) lines.push('', `➕ Adicionar (${opts.added.length})`, opts.added.join(', '));
  if (opts.removed.length) lines.push('', `➖ Remover (${opts.removed.length})`, opts.removed.join(', '));
  if (opts.missing.length) lines.push('', `❓ Não encontrei no banco`, opts.missing.join(', '));
  lines.push('', 'Use os botões abaixo para confirmar ou cancelar.');
  return lines.join('\n').trim();
}

export function formatConfirmRemoveMessage(opts: { beforeCount: number; toRemove: string[]; missing: string[] }): string {
  const lines: string[] = [];
  lines.push('⚠️ Confirmação necessária');
  lines.push(`Você está prestes a remover ${opts.toRemove.length} fundo(s) da sua lista (${opts.beforeCount} atuais)`);
  if (opts.toRemove.length) lines.push('', `➖ Remover`, opts.toRemove.join(', '));
  if (opts.missing.length) lines.push('', `❓ Não encontrei no banco`, opts.missing.join(', '));
  lines.push('', 'Use os botões abaixo para confirmar ou cancelar.');
  return lines.join('\n').trim();
}
