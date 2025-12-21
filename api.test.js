import assert from 'node:assert/strict';
import { test, beforeEach, afterEach } from 'node:test';

// Minimal browser shims for instrumentation and mock-data helpers
global.window = global.window || { location: { search: '' } };
if (!global.window.location) {
  global.window.location = { search: '' };
}
global.requestAnimationFrame = global.requestAnimationFrame || ((cb) => setTimeout(cb, 0));

const apiModule = await import('./api.js');
const instrumentationModule = await import('./instrumentation.js');

const { createApiRunner } = apiModule;
const { instrumentation } = instrumentationModule;

const defaultDates = { startDate: '2024-01-01', endDate: '2024-01-02', timezone: 'UTC' };

let originalFetch;

beforeEach(() => {
  originalFetch = global.fetch;
  instrumentation.reset();
  window.location.search = '';
});

afterEach(() => {
  global.fetch = originalFetch;
});

function createAbortError() {
  return typeof DOMException !== 'undefined'
    ? new DOMException('Aborted', 'AbortError')
    : Object.assign(new Error('Aborted'), { name: 'AbortError' });
}

function createFetchStub({
  lastPage = 5,
  latencyMs = 0,
  rowsPerPage = 1,
  onFetch,
} = {}) {
  return (url, { signal } = {}) => new Promise((resolve, reject) => {
    const page = Number(new URL(url).searchParams.get('page')) || 1;
    onFetch?.(page);

    const timer = setTimeout(() => {
      if (signal?.aborted) {
        reject(createAbortError());
        return;
      }
      const data = Array.from({ length: rowsPerPage }, (_, i) => ({ id: `${page}-${i}` }));
      const payload = {
        current_page: page,
        last_page: lastPage,
        next_page_url: page < lastPage ? `mock://next/${page + 1}` : null,
        data,
      };
      resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        text: async () => JSON.stringify(payload),
      });
    }, latencyMs);

    if (signal) {
      const onAbort = () => {
        clearTimeout(timer);
        reject(createAbortError());
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}

async function runPipeline({
  lastPage = 6,
  fetchLatencyMs = 0,
  rowsPerPage = 1,
  processingDelayMs = 0,
  pipelineConfig = {},
  abortAfterPage = null,
} = {}) {
  const progressPages = [];
  const pipelineSnapshots = [];
  const abortCtrl = new AbortController();

  global.fetch = createFetchStub({
    lastPage,
    latencyMs: fetchLatencyMs,
    rowsPerPage,
  });

  const apiRunner = createApiRunner({
    tenant: 'tenant',
    tokenGetter: () => 'token',
    mockMode: false,
    signal: abortCtrl.signal,
    pipelineConfig,
    onProgress: ({ page }) => {
      progressPages.push(page);
      if (abortAfterPage && page >= abortAfterPage) {
        abortCtrl.abort('test abort');
      }
    },
    onPerf: (payload) => {
      if (payload?.type === 'pipeline') {
        pipelineSnapshots.push(payload);
      }
    },
  });

  const runPromise = apiRunner.run({
    reports: ['detention_history'],
    facilities: ['FAC1'],
    ...defaultDates,
    onRows: async ({ rows }) => {
      if (processingDelayMs) {
        await new Promise((resolve) => setTimeout(resolve, processingDelayMs));
      }
      return rows;
    },
  });

  return { runPromise, abortCtrl, progressPages, pipelineSnapshots };
}

test('prefetch buffer stays bounded by configured maximum', async () => {
  const { runPromise, pipelineSnapshots } = await runPipeline({
    lastPage: 12,
    fetchLatencyMs: 0,
    pipelineConfig: { fetchBufferMax: 3, processingPoolMax: 1 },
  });

  const results = await runPromise;
  const stats = results[0].stats;
  const maxBufferedObserved = pipelineSnapshots.reduce(
    (max, snap) => Math.max(max, snap.fetchBuffer ?? 0),
    0
  );
  assert.ok(stats.maxFetchBuffer <= 3, `maxFetchBuffer ${stats.maxFetchBuffer} exceeded cap`);
  assert.ok(stats.maxBufferedPlusInFlight <= 3, 'buffer + inflight exceeded fetch cap');
  assert.ok(maxBufferedObserved <= 3, 'pipeline snapshot exceeded fetch cap');
});

test('processing concurrency respects configured pool limit even when fetch is fast', async () => {
  const fetchCalls = [];
  const { runPromise } = await runPipeline({
    lastPage: 10,
    fetchLatencyMs: 0,
    pipelineConfig: { processingPoolMax: 2, fetchBufferMax: 4 },
  });
  const results = await runPromise;
  const stats = results[0].stats;
  assert.ok(stats.maxProcessingActive <= 2, `processing active ${stats.maxProcessingActive} exceeded cap`);
});

test('slow processing fills buffer without unbounded fetch growth', async () => {
  const fetchCounts = [];
  global.fetch = createFetchStub({
    lastPage: 8,
    latencyMs: 0,
    onFetch: (page) => fetchCounts.push(page),
  });

  const apiRunner = createApiRunner({
    tenant: 'tenant',
    tokenGetter: () => 'token',
    mockMode: false,
    signal: new AbortController().signal,
    pipelineConfig: { fetchBufferMax: 4, processingPoolMax: 1 },
  });

  const results = await apiRunner.run({
    reports: ['detention_history'],
    facilities: ['FAC1'],
    ...defaultDates,
    onRows: async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    },
  });

  const stats = results[0].stats;
  assert.ok(stats.maxBufferedPlusInFlight <= 4, 'buffer should stop at cap during slow processing');
  assert.ok(fetchCounts.length >= 8, 'all pages should still be fetched');
});

test('slow fetch does not exceed processing cap and completes without deadlock', async () => {
  const { runPromise } = await runPipeline({
    lastPage: 6,
    fetchLatencyMs: 15,
    pipelineConfig: { processingPoolMax: 2, fetchBufferMax: 3 },
  });

  const results = await runPromise;
  const stats = results[0].stats;
  assert.ok(stats.maxProcessingActive <= 2, 'processing cap should be honored when fetch is slow');
  assert.equal(stats.finalBuffer, 0, 'buffer should drain at completion');
  assert.equal(stats.finalProcessing, 0, 'processing set should be empty at completion');
});

test('abort stops new processing and fetching quickly', async () => {
  const { runPromise, abortCtrl, progressPages } = await runPipeline({
    lastPage: 20,
    fetchLatencyMs: 5,
    processingDelayMs: 15,
    abortAfterPage: 3,
    pipelineConfig: { fetchBufferMax: 4, processingPoolMax: 2 },
  });

  await assert.rejects(runPromise, /Aborted|abort/i);
  assert.ok(progressPages.length <= 5, 'progress should stop soon after abort');
  assert.ok(abortCtrl.signal.aborted, 'abort controller should be triggered');
});

test('large runs yield at least once per page', async () => {
  let yielded = 0;
  const { runPromise } = await runPipeline({
    lastPage: 50,
    fetchLatencyMs: 0,
    rowsPerPage: 0,
    pipelineConfig: {
      fetchBufferMax: 5,
      processingPoolMax: 2,
      yieldFn: async () => { yielded += 1; },
      yieldPageThreshold: 40,
    },
  });
  const results = await runPromise;
  const stats = results[0].stats;
  assert.ok(stats.progressEvents >= 50, 'all pages should report progress');
  assert.ok(yielded >= 50, `expected at least 50 yields, saw ${yielded}`);
});

test('no retained buffer after completion', async () => {
  const { runPromise } = await runPipeline({
    lastPage: 5,
    fetchLatencyMs: 0,
    rowsPerPage: 2,
    pipelineConfig: { fetchBufferMax: 3, processingPoolMax: 2 },
  });
  const results = await runPromise;
  const stats = results[0].stats;
  assert.equal(stats.finalBuffer, 0, 'prefetch buffer should be empty at completion');
  assert.equal(stats.finalProcessing, 0, 'processing pool should be empty at completion');
});
