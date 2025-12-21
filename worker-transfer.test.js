import assert from 'node:assert/strict';
import { test } from 'node:test';
import { sanitizeRowsForWorker, createWorkerBatcher } from './worker-transfer.js';
import { normalizeRowStrict } from './analysis.js';

test('sanitizeRowsForWorker strips phone/cell values but preserves presence', () => {
  const raw = [
    { id: 1, phone: '123-456-7890', driver_cell: '999', name: 'A' },
    { id: 2, Phone: '', facility: 'F1' },
  ];

  const sanitized = sanitizeRowsForWorker(raw);
  assert.equal(sanitized.length, 2);
  assert.strictEqual(sanitized[0].phone, true, 'phone value should be redacted to presence only');
  assert.strictEqual(sanitized[0].driver_cell, true, 'cell value should be redacted to presence only');
  assert.ok(!('Phone' in sanitized[1]), 'empty phone-like fields should be omitted entirely');
  assert.equal(sanitized[0].name, 'A');
});

test('worker batcher groups pages and keeps rows ready for normalization', async () => {
  const messages = [];
  const batcher = createWorkerBatcher({
    runId: 'run1',
    postMessage: (payload) => messages.push(payload),
    flushIntervalMs: 1,
  });

  await batcher.enqueue({
    report: 'detention_history',
    facility: 'FAC1',
    page: 1,
    lastPage: 2,
    rows: [{ id: 1, phone: '321', value: 'x' }],
  });
  await batcher.enqueue({
    report: 'detention_history',
    facility: 'FAC1',
    page: 2,
    lastPage: 2,
    rows: [{ id: 2, status: 'ok' }],
  });

  await batcher.flush();
  assert.equal(messages.length, 1, 'pages should be batched into a single message');
  const payload = messages[0];
  assert.equal(payload.type, 'PAGE_ROWS_BATCH');
  assert.equal(payload.pages.length, 2);

  const normalized = payload.pages[0].rows.map((row) => normalizeRowStrict(row, {
    report: 'detention_history',
    timezone: 'UTC',
  }));
  assert.ok(normalized[0].flags.anyPhoneFieldPresent, 'presence flag should be preserved after redaction');
  assert.equal(normalized[0].row.value, 'x');
});

test('worker batcher stops posting after abort', async () => {
  const messages = [];
  const abortCtrl = new AbortController();
  const batcher = createWorkerBatcher({
    runId: 'run2',
    postMessage: (payload) => messages.push(payload),
    signal: abortCtrl.signal,
  });

  abortCtrl.abort('stop');
  await batcher.enqueue({
    report: 'dockdoor_history',
    facility: 'FAC1',
    page: 1,
    lastPage: 1,
    rows: [{ id: 1, phone: '111' }],
  });
  await batcher.flush();

  assert.equal(messages.length, 0, 'no batches should be posted once aborted');
});
