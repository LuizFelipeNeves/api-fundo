import test from 'node:test';
import assert from 'node:assert/strict';
import { parseBotCommand } from './commands';

test('parseBotCommand não interpreta typo /contation como set', () => {
  const cmd = parseBotCommand('/contation TELM11');
  assert.deepEqual(cmd, { kind: 'cotation', code: 'TELM11' });
});

test('parseBotCommand não usa /set para comandos desconhecidos com /', () => {
  const cmd = parseBotCommand('/qualquercoisa TELM11');
  assert.deepEqual(cmd, { kind: 'help' });
});

test('parseBotCommand entende /confirm e /cancel', () => {
  assert.deepEqual(parseBotCommand('/confirm'), { kind: 'confirm' });
  assert.deepEqual(parseBotCommand('/cancel'), { kind: 'cancel' });
});

test('parseBotCommand entende /resumo-documento', () => {
  assert.deepEqual(parseBotCommand('/resumo-documento'), { kind: 'resumo-documento', codes: [] });
  assert.deepEqual(parseBotCommand('/resumo_documento HGLG11 MXRF11'), { kind: 'resumo-documento', codes: ['HGLG11', 'MXRF11'] });
});

test('parseBotCommand entende /export', () => {
  assert.deepEqual(parseBotCommand('/export'), { kind: 'export', codes: [] });
  assert.deepEqual(parseBotCommand('/export HGLG11 MXRF11'), { kind: 'export', codes: ['HGLG11', 'MXRF11'] });
  assert.deepEqual(parseBotCommand('/exportar hglg11'), { kind: 'export', codes: ['HGLG11'] });
});
