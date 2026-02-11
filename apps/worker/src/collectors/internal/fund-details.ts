import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchFIIDetails } from '../../services/client';

export const fundDetailsCollector: Collector = {
  name: 'fund_details',
  supports(request: CollectRequest) {
    return request.collector === 'fund_details' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const { details } = await fetchFIIDetails(code);
    return {
      collector: 'fund_details',
      fetched_at: new Date().toISOString(),
      payload: {
        persist_request: {
          type: 'fund_details',
          item: {
            code,
            id: details.id,
            cnpj: details.cnpj,
            razao_social: details.razao_social,
            publico_alvo: details.publico_alvo,
            mandato: details.mandato,
            segmento: details.segmento,
            tipo_fundo: details.tipo_fundo,
            prazo_duracao: details.prazo_duracao,
            tipo_gestao: details.tipo_gestao,
            taxa_adminstracao: details.taxa_adminstracao,
            daily_liquidity: details.daily_liquidity ?? null,
            vacancia: details.vacancia,
            numero_cotistas: details.numero_cotistas,
            cotas_emitidas: details.cotas_emitidas,
            valor_patrimonial_cota: details.valor_patrimonial_cota,
            valor_patrimonial: details.valor_patrimonial,
            ultimo_rendimento: details.ultimo_rendimento,
          },
        },
      },
    };
  },
};
