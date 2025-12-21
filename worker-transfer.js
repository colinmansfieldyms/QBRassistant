const PII_KEY_RE = /(cell|phone)/i;
const DEFAULT_BATCH_ROWS = 600;
const DEFAULT_BATCH_PAGES = 3;
const DEFAULT_FLUSH_INTERVAL_MS = 10;
const MAX_BUFFERED_ROWS = 1600;

function hasValue(v) {
  return v !== null && v !== undefined && v !== '';
}

export function sanitizeRowsForWorker(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const sanitized = new Array(rows.length);

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (!row || typeof row !== 'object') {
      sanitized[i] = {};
      continue;
    }

    const clean = Object.create(null);
    for (const key in row) {
      if (!Object.prototype.hasOwnProperty.call(row, key)) continue;
      const value = row[key];
      if (PII_KEY_RE.test(key)) {
        if (hasValue(value)) {
          // Preserve presence only; never forward raw phone/cell values across threads.
          clean[key] = true;
        }
        continue;
      }
      clean[key] = value;
    }
    sanitized[i] = clean;
  }

  return sanitized;
}

export function createWorkerBatcher({
  runId,
  postMessage,
  signal,
  maxBatchRows = DEFAULT_BATCH_ROWS,
  maxBatchPages = DEFAULT_BATCH_PAGES,
  flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS,
} = {}) {
  if (!runId) throw new Error('runId is required for worker batcher');
  if (typeof postMessage !== 'function') throw new Error('postMessage callback is required');

  let buffer = [];
  let bufferedRows = 0;
  let flushTimer = null;
  let stopped = false;
  let batchId = 0;

  const clearTimer = () => {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
  };

  const flush = () => {
    if (stopped || !buffer.length) return Promise.resolve(null);
    clearTimer();
    const pages = buffer;
    buffer = [];
    bufferedRows = 0;
    batchId += 1;
    // postMessage is synchronous in browsers; keep return signature Promise for symmetry/tests.
    postMessage({ type: 'PAGE_ROWS_BATCH', runId, batchId, pages });
    return Promise.resolve({ batchId, pages });
  };

  const scheduleFlush = () => {
    if (flushTimer || stopped) return;
    flushTimer = setTimeout(() => {
      flushTimer = null;
      flush();
    }, flushIntervalMs);
  };

  const enqueue = async ({ report, facility, page, lastPage, rows }) => {
    if (stopped || signal?.aborted) return;
    const sanitizedRows = sanitizeRowsForWorker(rows);
    buffer.push({ report, facility, page, lastPage, rows: sanitizedRows });
    bufferedRows += sanitizedRows.length;

    const overRowLimit = bufferedRows >= Math.min(MAX_BUFFERED_ROWS, Math.max(maxBatchRows, 1));
    if (buffer.length >= maxBatchPages || overRowLimit) {
      await flush();
    } else {
      scheduleFlush();
    }
  };

  const stop = async () => {
    stopped = true;
    clearTimer();
    await flush();
  };

  return { enqueue, flush, stop };
}
