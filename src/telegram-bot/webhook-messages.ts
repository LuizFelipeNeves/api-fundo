function normalizeCategoryKey(value: string): string {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
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
  return funds.length ? `Sua lista (${funds.length} fundos): ${funds.join(', ')}` : 'Sua lista está vazia.';
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

export function formatSetMessage(opts: {
  existing: string[];
  added: string[];
  removed: string[];
  missing: string[];
}): string {
  const parts = [
    opts.existing.length ? `Lista atualizada. Total: ${opts.existing.length} fundos.` : 'Lista atualizada. Sua lista está vazia.',
  ];
  if (opts.existing.length) parts.push(`Fundos: ${opts.existing.join(', ')}`);
  if (opts.added.length) parts.push(`Adicionados: ${opts.added.join(', ')}`);
  if (opts.removed.length) parts.push(`Removidos: ${opts.removed.join(', ')}`);
  if (opts.missing.length) parts.push(`Não encontrei no banco: ${opts.missing.join(', ')}`);
  return parts.join('\n');
}

export function formatAddMessage(opts: { addedCount: number; nowList: string[]; missing: string[] }): string {
  const parts = [`Adicionados: ${opts.addedCount}`];
  parts.push(opts.nowList.length ? `Agora (${opts.nowList.length} fundos): ${opts.nowList.join(', ')}` : 'Agora: (vazia)');
  if (opts.missing.length) parts.push(`Não encontrei no banco: ${opts.missing.join(', ')}`);
  return parts.join('\n');
}

export function formatRemoveMessage(opts: { removedCount: number; nowList: string[]; missing: string[] }): string {
  const parts = [`Removidos: ${opts.removedCount}`];
  parts.push(opts.nowList.length ? `Agora (${opts.nowList.length} fundos): ${opts.nowList.join(', ')}` : 'Agora: (vazia)');
  if (opts.missing.length) parts.push(`Não encontrei no banco: ${opts.missing.join(', ')}`);
  return parts.join('\n');
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

export function formatDocumentsMessage(opts: { docs: LatestDocumentRow[]; limit: number; code?: string }): string {
  if (!opts.docs.length) {
    return opts.code ? `Não encontrei documentos para ${opts.code}.` : 'Não encontrei documentos para sua lista.';
  }

  const header = opts.code
    ? `Últimos documentos (${opts.docs.length}/${opts.limit}) - ${opts.code}`
    : `Últimos documentos (${opts.docs.length}/${opts.limit})`;

  const lines: string[] = [header, ''];
  for (const d of opts.docs) {
    const code = String(d.fund_code || '').toUpperCase();
    const title = String(d.title || '').trim();
    const date = String(d.dateUpload || '').trim();
    const docType = [String(d.category || '').trim(), String(d.type || '').trim()].filter(Boolean).join(' · ');
    const url = String(d.url || '').trim();
    lines.push(`${code}${date ? ` ${date}` : ''}${docType ? ` · ${docType}` : ''}`);
    if (title) lines.push(title);
    if (url) lines.push(url);
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
  counts?: { documents?: number; dividends?: number; cotations?: number };
}): string {
  const f = opts.fund;
  const code = f.code.toUpperCase();

  const lines: string[] = [`📌 ${code}`];
  const name = String(f.razao_social || '').trim();
  if (name) lines.push(name);
  const cnpj = String(f.cnpj || '').trim();
  if (cnpj) lines.push(`CNPJ: ${cnpj}`);

  const cat = [String(f.segmento || '').trim(), String(f.tipo_fundo || '').trim(), String(f.sector || '').trim(), String(f.type || '').trim()]
    .filter(Boolean)
    .join(' · ');
  if (cat) lines.push(`Categoria: ${cat}`);

  const line1: string[] = [];
  if (f.p_vp !== null && f.p_vp !== undefined) line1.push(`P/VP: ${formatNumber(f.p_vp, 2)}`);
  if (f.daily_liquidity !== null && f.daily_liquidity !== undefined) line1.push(`Liquidez: ${formatNumber(f.daily_liquidity, 0)}`);
  if (f.net_worth !== null && f.net_worth !== undefined) line1.push(`PL: ${formatNumber(f.net_worth, 0)}`);
  if (line1.length) lines.push(line1.join(' | '));

  const line2: string[] = [];
  if (f.dividend_yield !== null && f.dividend_yield !== undefined) line2.push(`DY: ${formatNumber(f.dividend_yield, 2)}`);
  if (f.dividend_yield_last_5_years !== null && f.dividend_yield_last_5_years !== undefined) line2.push(`DY 5a: ${formatNumber(f.dividend_yield_last_5_years, 2)}`);
  if (f.ultimo_rendimento !== null && f.ultimo_rendimento !== undefined) line2.push(`Últ. rend.: ${formatNumber(f.ultimo_rendimento, 4)}`);
  if (line2.length) lines.push(line2.join(' | '));

  const line3: string[] = [];
  const vac = f.vacancia;
  if (vac !== null && vac !== undefined) line3.push(`Vacância: ${formatNumber(vac, 2)}`);
  if (f.numero_cotistas !== null && f.numero_cotistas !== undefined) line3.push(`Cotistas: ${formatNumber(f.numero_cotistas, 0)}`);
  if (f.cotas_emitidas !== null && f.cotas_emitidas !== undefined) line3.push(`Cotas: ${formatNumber(f.cotas_emitidas, 0)}`);
  if (line3.length) lines.push(line3.join(' | '));

  const line4: string[] = [];
  if (f.valor_patrimonial_cota !== null && f.valor_patrimonial_cota !== undefined)
    line4.push(`VP/cota: ${formatNumber(f.valor_patrimonial_cota, 2)}`);
  if (f.valor_patrimonial !== null && f.valor_patrimonial !== undefined) line4.push(`VP: ${formatNumber(f.valor_patrimonial, 0)}`);
  if (line4.length) lines.push(line4.join(' | '));

  const extra = [
    String(f.publico_alvo || '').trim() ? `Público: ${String(f.publico_alvo || '').trim()}` : '',
    String(f.mandato || '').trim() ? `Mandato: ${String(f.mandato || '').trim()}` : '',
    String(f.tipo_gestao || '').trim() ? `Gestão: ${String(f.tipo_gestao || '').trim()}` : '',
    String(f.prazo_duracao || '').trim() ? `Prazo: ${String(f.prazo_duracao || '').trim()}` : '',
    String(f.taxa_adminstracao || '').trim() ? `Taxa adm.: ${String(f.taxa_adminstracao || '').trim()}` : '',
  ].filter(Boolean);
  lines.push(...extra);

  const counts = opts.counts;
  if (counts) {
    const parts: string[] = [];
    if (counts.documents !== undefined) parts.push(`Docs: ${formatNumber(counts.documents, 0)}`);
    if (counts.dividends !== undefined) parts.push(`Proventos: ${formatNumber(counts.dividends, 0)}`);
    if (counts.cotations !== undefined) parts.push(`Cotações: ${formatNumber(counts.cotations, 0)}`);
    if (parts.length) lines.push(parts.join(' | '));
  }

  const updatedAt = String(f.updated_at || '').trim();
  if (updatedAt) lines.push(`Atualizado: ${updatedAt}`);

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
  const lines: string[] = [`📈 ${code}`, `Data base: ${opts.asOfDateIso}`, `Último preço: ${formatNumber(opts.lastPrice, 4)}`, ''];

  lines.push(
    `Variações: 7d ${formatPercent(opts.returns.d7)} | 30d ${formatPercent(opts.returns.d30)} | 90d ${formatPercent(opts.returns.d90)}`
  );

  lines.push(`Drawdown máx: ${formatPercent(opts.drawdown.max)}`);

  const v30 = opts.volatility.d30 === null ? '—' : `${(opts.volatility.d30 * 100).toFixed(2)}%`;
  const v90 = opts.volatility.d90 === null ? '—' : `${(opts.volatility.d90 * 100).toFixed(2)}%`;
  lines.push(`Volatilidade (anualizada): 30d ${v30} | 90d ${v90}`);

  lines.push(`Cache: ${opts.computedAt}`);
  return lines.join('\n').trim();
}
