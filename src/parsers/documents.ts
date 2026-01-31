export interface DocumentData {
  id: number;
  title: string;
  category: string;
  type: string;
  date: string;
  dateUpload: string;
  url: string;
  status: string;
  version: number;
}

export function normalizeDocuments(raw: any[]): DocumentData[] {
  return raw.map((item) => ({
    id: item.id,
    title: item.descricaoFundo || '',
    category: item.categoriaDocumento || '',
    type: item.tipoDocumento || '',
    date: item.dataReferencia || '',
    dateUpload: item.dataEntrega || '',
    url: `https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id=${item.id}&cvm=true&`,
    status: item.descricaoStatus || '',
    version: item.versao || 1,
  }));
}
