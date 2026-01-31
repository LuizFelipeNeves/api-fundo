import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeFIIDetails } from './fii-details';

test('normalizeFIIDetails normaliza casas decimais e inteiros', () => {
  const details = normalizeFIIDetails(
    {
      cnpj: '00.000.000/0000-00',
      vacancia: '1,2345%',
      numero_cotistas: '1.234,9',
      cotas_emitidas: '2.345,1',
      valor_patrimonial_cota: 'R$ 1,23456',
      valor_patrimonial: 'R$ 10,47 M',
      ultimo_rendimento: '0,123456',
    } as any,
    'ABCD11',
    '1'
  );

  assert.equal(details.vacancia, 1.23);
  assert.equal(details.numero_cotistas, 1235);
  assert.equal(details.cotas_emitidas, 2345);
  assert.equal(details.valor_patrimonial_cota, 1.23);
  assert.equal(details.valor_patrimonial, 10470000);
  assert.equal(details.ultimo_rendimento, 0.1235);
});
