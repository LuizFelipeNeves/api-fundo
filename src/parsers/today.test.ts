import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeCotationsToday } from './today';

test('normalizeCotationsToday normaliza formato investidor10', () => {
  const raw = { real: [{ price: 10.5, created_at: '2026-02-02 10:01:00' }] };
  assert.deepEqual(normalizeCotationsToday(raw), [{ price: 10.5, hour: '10:01' }]);
});

test('normalizeCotationsToday ordena e remove duplicados por hora no formato investidor10', () => {
  const raw = {
    real: [
      { price: 10, created_at: '2026-02-02 10:02:00' },
      { price: 9, created_at: '2026-02-02 10:01:00' },
      { price: 11, created_at: '2026-02-02 10:02:59' },
    ],
  };

  assert.deepEqual(normalizeCotationsToday(raw), [
    { price: 9, hour: '10:01' },
    { price: 11, hour: '10:02' },
  ]);
});

test('normalizeCotationsToday normaliza formato statusinvest', () => {
  const raw = [
    {
      currencyType: 1,
      currency: 'Real brasileiro',
      symbol: 'R$',
      prices: [
        { price: 6.62, date: '02/02/2026 10:05:00' },
        { value: '6,63', hour: '10:06' },
      ],
    },
  ];

  assert.deepEqual(normalizeCotationsToday(raw), [
    { price: 6.62, hour: '10:05' },
    { price: 6.63, hour: '10:06' },
  ]);
});

test('normalizeCotationsToday ordena e remove duplicados por hora no formato statusinvest', () => {
  const raw = [
    {
      currencyType: 1,
      prices: [{ value: '6,61', hour: '10:05' }, { price: 6.6, date: '02/02/2026 10:04:00' }, { price: 6.62, hour: '10:05' }],
    },
  ];

  assert.deepEqual(normalizeCotationsToday(raw), [
    { price: 6.6, hour: '10:04' },
    { price: 6.62, hour: '10:05' },
  ]);
});
