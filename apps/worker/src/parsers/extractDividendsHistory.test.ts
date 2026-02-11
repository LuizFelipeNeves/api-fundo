import test from 'node:test';
import assert from 'node:assert/strict';
import { extractDividendsHistory } from './index';

test('extractDividendsHistory extrai apenas a tabela table-dividends-history e deduplica', () => {
  const html = `
    <table id="other-table">
      <tbody>
        <tr>
          <td class="text-center">Dividendos</td>
          <td class="text-center">31/01/2026</td>
          <td class="text-center">12/02/2026</td>
          <td class="text-center">1,20000000</td>
        </tr>
      </tbody>
    </table>

    <table id="table-dividends-history" class="table table-balance table-dividends-history" style="width: 100%">
      <thead>
        <tr>
          <th class="text-center"><h3>tipo</h3></th>
          <th class="text-center"><h3>data com</h3></th>
          <th class="text-center"><h3>pagamento</h3></th>
          <th class="text-center"><h3>valor</h3></th>
        </tr>
      </thead>
      <tbody>
        <tr class="visible-even">
          <td class="text-center">Dividendos</td>
          <td class="text-center">30/01/2026</td>
          <td class="text-center">12/02/2026</td>
          <td class="text-center">1,20000000</td>
        </tr>
        <tr class="visible-even">
          <td class="text-center">Dividendos</td>
          <td class="text-center">30/01/2026</td>
          <td class="text-center">12/02/2026</td>
          <td class="text-center">1,20000000</td>
        </tr>
        <tr class="visible-odd">
          <td class="text-center">Dividendos</td>
          <td class="text-center">30/12/2025</td>
          <td class="text-center">14/01/2026</td>
          <td class="text-center">1,30000000</td>
        </tr>
      </tbody>
    </table>
  `;

  assert.deepEqual(extractDividendsHistory(html), [
    { type: 'Dividendos', date: '30/01/2026', payment: '12/02/2026', value: 1.2 },
    { type: 'Dividendos', date: '30/12/2025', payment: '14/01/2026', value: 1.3 },
  ]);
});

