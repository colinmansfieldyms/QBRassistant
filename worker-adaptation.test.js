import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  WORKER_AUTO_THRESHOLD_PAGES,
  CHUNK_SIZE_DEFAULT,
  CHUNK_SIZE_MAX,
  PARTIAL_EMIT_INTERVAL_MS_DEFAULT,
  PARTIAL_EMIT_INTERVAL_MS_HEAVY,
  createAdaptiveState,
  updateChunkSizing,
  updatePartialInterval,
  shouldAutoUseWorker,
  computeRenderThrottle,
} from './worker-adaptation.js';
import { parseTimestamp, normalizeRowStrict, setDateTimeImplementation } from './analysis.js';

class StubDateTime {
  constructor(date, zone = 'utc') {
    this.date = date;
    this.zone = zone;
    this.isValid = !Number.isNaN(date?.getTime?.());
    this.offset = -date.getTimezoneOffset();
  }
  static fromMillis(ms, { zone } = {}) {
    return new StubDateTime(new Date(ms), zone || 'utc');
  }
  static fromObject(obj = {}, { zone } = {}) {
    const { year, month = 1, day = 1, hour = 0, minute = 0, second = 0 } = obj;
    return new StubDateTime(new Date(Date.UTC(year, (month || 1) - 1, day || 1, hour, minute, second)), zone || 'utc');
  }
  static fromISO(str, { zone } = {}) {
    return new StubDateTime(new Date(str), zone || 'utc');
  }
  static fromSQL(str, { zone } = {}) {
    return StubDateTime.fromISO(str, { zone });
  }
  static fromFormat(str, _fmt, { zone } = {}) {
    return StubDateTime.fromISO(str, { zone });
  }
  setZone(zone) {
    return new StubDateTime(this.date, zone || this.zone);
  }
  toFormat() {
    return this.date.toISOString();
  }
}

setDateTimeImplementation(StubDateTime);

test('auto worker enablement prefers worker when pages exceed threshold', () => {
  const decision = shouldAutoUseWorker({
    estimatedPages: WORKER_AUTO_THRESHOLD_PAGES + 10,
    workerAvailable: true,
    preferred: false,
  });
  assert.equal(decision, true, 'Large runs should auto-enable worker even if preference is off');

  const smallDecision = shouldAutoUseWorker({
    estimatedPages: 10,
    workerAvailable: true,
    preferred: false,
  });
  assert.equal(smallDecision, false, 'Small runs should respect preference');
});

test('chunk size increases only after sustained headroom and decreases on overload', () => {
  let state = createAdaptiveState(0);
  // Simulate headroom with short chunk times and low backlog
  for (let i = 0; i < 4; i++) {
    state = updateChunkSizing(state, { chunkMs: 5, backlog: 0, now: 1500 + i * 100 });
  }
  assert.ok(state.chunkSize > CHUNK_SIZE_DEFAULT, 'Chunk size should grow with sustained headroom');

  // Simulate overload to force reduction
  const currentSize = state.chunkSize;
  for (let i = 0; i < 3; i++) {
    state = updateChunkSizing(state, { chunkMs: 25, backlog: 10, now: 4000 + i * 500 });
  }
  assert.ok(state.chunkSize <= currentSize, 'Chunk size should reduce when overloaded');
  assert.ok(state.chunkSize >= CHUNK_SIZE_DEFAULT, 'Chunk size should not drop below default');
});

test('partial emission interval stretches under heavy backlog and recovers later', () => {
  let state = createAdaptiveState(0);
  state.partialIntervalMs = PARTIAL_EMIT_INTERVAL_MS_DEFAULT;

  state = updatePartialInterval(state, { backlog: 8, chunkSize: CHUNK_SIZE_MAX, now: 1000 });
  assert.equal(state.partialIntervalMs, PARTIAL_EMIT_INTERVAL_MS_HEAVY, 'Heavy backlog should slow partial cadence');

  state = updatePartialInterval(state, { backlog: 0, chunkSize: CHUNK_SIZE_DEFAULT, now: 2000 + 1000 });
  assert.equal(state.partialIntervalMs, PARTIAL_EMIT_INTERVAL_MS_DEFAULT, 'Cadence should recover when load lightens');
});

test('render throttle clamps to partial cadence window', () => {
  const slow = computeRenderThrottle(PARTIAL_EMIT_INTERVAL_MS_HEAVY);
  const fast = computeRenderThrottle(50);
  assert.ok(slow >= PARTIAL_EMIT_INTERVAL_MS_DEFAULT, 'Render throttle should follow slower partial interval');
  assert.ok(fast >= PARTIAL_EMIT_INTERVAL_MS_DEFAULT, 'Render throttle should not run faster than default cadence');
});

test('PII scrubber invariant: phone fields are dropped during normalization', () => {
  const normalized = normalizeRowStrict(
    { phone: '123', driver_cell: '555', other: 'keep' },
    { report: 'driver_history', timezone: 'UTC' },
  );
  assert.ok(!normalized.row.phone, 'phone value should be removed');
  assert.ok(!normalized.row.driver_cell, 'cell value should be removed');
  assert.equal(normalized.row.other, 'keep');
});

test('timestamp parsing honors treatAsLocal exception', () => {
  const utcTs = parseTimestamp('2024-01-01T00:00:00Z', { timezone: 'America/Chicago', assumeUTC: true });
  const localTs = parseTimestamp('2024-01-01 00:00', { timezone: 'America/Chicago', treatAsLocal: true });
  assert.ok(utcTs?.isValid, 'UTC timestamp should parse');
  assert.ok(localTs?.isValid, 'Local timestamp should parse');
  assert.equal(localTs.zone, 'America/Chicago', 'Local timestamp should retain facility timezone');
});
