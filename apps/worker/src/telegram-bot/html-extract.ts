import cheerio from 'cheerio';

export function extract(html: string): Record<string, string> {
  const $ = cheerio.load(html);
  const dados: Record<string, string> = {};

  $('script, style').remove();

  $('table').each((_, table) => {
    $(table)
      .find('tr')
      .each((_, tr) => {
        const linhaTexto: string[] = [];

        $(tr)
          .find('td, th')
          .each((_, td) => {
            const texto = $(td).text().trim().replace(/\s\s+/g, ' ');
            if (texto !== '') {
              linhaTexto.push(texto);
            }
          });

        for (let i = 0; i < linhaTexto.length; i++) {
          const item = linhaTexto[i];
          if (i + 1 < linhaTexto.length) {
            const chave = item.replace(/:$/, '').trim();
            const valor = linhaTexto[i + 1];
            if (chave.length < 100 && chave !== valor) {
              dados[chave] = valor;
              i++;
            }
          }
        }
      });
  });

  return dados;
}
