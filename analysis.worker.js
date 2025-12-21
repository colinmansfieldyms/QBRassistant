import { createAnalyzers, normalizeRowStrict, setDateTimeImplementation } from './analysis.js?v=2025.01.07.0';
import { DateTime } from 'https://cdn.jsdelivr.net/npm/luxon@3.5.0/build/es6/luxon.js';
import {
  CHUNK_SIZE_DEFAULT,
  CHUNK_SIZE_MAX,
  PARTIAL_EMIT_INTERVAL_MS_DEFAULT,
  createAdaptiveState,
  updateChunkSizing,
  updatePartialInterval,
} from './worker-adaptation.js';

setDateTimeImplementation(DateTime);

const runs = new Map();

function post(type, payload = {}) {
  self.postMessage({ type, ...payload });
}

function bufferWarning(run, msg) {
  run.warnings.push(msg);
  run.warningBuffer.push(msg);
}

function flushWarnings(run) {
  const copy = run.warningBuffer.slice();
  run.warningBuffer.length = 0;
  return copy;
}

function collectParseStats(run) {
  let ok = 0;
  let fails = 0;
  for (const analyzer of Object.values(run.analyzers)) {
    ok += analyzer.parseOk || 0;
    fails += analyzer.parseFails || 0;
  }
  run.parseOk = ok;
  run.parseFails = fails;
  return { parseOk: ok, parseFails: fails };
}

function buildResults(run) {
  const meta = {
    tenant: run.config.tenant,
    facilities: run.config.facilities,
    startDate: run.config.startDate,
    endDate: run.config.endDate,
    timezone: run.config.timezone,
    assumptions: run.config.assumptions,
    roiEnabled: run.config.roiEnabled,
  };

  const results = {};
  for (const report of run.config.selectedReports) {
    const analyzer = run.analyzers[report];
    if (!analyzer) continue;
    results[report] = analyzer.finalize(meta);
  }
  return results;
}

function handleInit(data) {
  const { runId, timezone, startDate, endDate, assumptions, selectedReports, facilities, tenant, roiEnabled } = data;
  if (!runId) return;

  const run = {
    analyzers: null,
    config: { timezone, startDate, endDate, assumptions, selectedReports, facilities, tenant, roiEnabled },
    warnings: [],
    warningBuffer: [],
    totalRowsProcessed: 0,
    parseOk: 0,
    parseFails: 0,
    cancelled: false,
    lastPartialAt: 0,
    backlogPages: 0,
    adaptive: createAdaptiveState(),
  };

  run.analyzers = createAnalyzers({
    timezone,
    startDate,
    endDate,
    assumptions,
    onWarning: (msg) => bufferWarning(run, msg),
  });

  runs.set(runId, run);
}

async function handlePageRows(data) {
  const { runId, report, facility, page, lastPage, rows } = data;
  const run = runs.get(runId);
  if (!run || run.cancelled) return;

  const analyzer = run.analyzers[report];
  if (!analyzer) return;

  const payloadRows = Array.isArray(rows) ? rows : [];
  let processed = 0;

  const backlog = Math.max(0, run.backlogPages - 1);
  const chunkSize = run.adaptive.chunkSize || CHUNK_SIZE_DEFAULT;

  for (let i = 0; i < payloadRows.length; i += chunkSize) {
    if (run.cancelled) break;
    const slice = payloadRows.slice(i, i + chunkSize);
    const chunkStart = performance.now();
    for (const raw of slice) {
      if (run.cancelled) break;
      const normalized = normalizeRowStrict(raw, {
        report,
        timezone: run.config.timezone,
        onWarning: (msg) => bufferWarning(run, msg),
      });
      if (normalized) {
        analyzer.ingest(normalized);
        processed++;
      }
    }

    const chunkDuration = performance.now() - chunkStart;
    run.adaptive = updateChunkSizing(run.adaptive, {
      chunkMs: chunkDuration,
      backlog,
      now: performance.now(),
    });
    run.adaptive = updatePartialInterval(run.adaptive, {
      backlog,
      chunkSize: run.adaptive.chunkSize,
      now: performance.now(),
    });

    if (payloadRows.length > chunkSize) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  if (run.cancelled) return;

  run.totalRowsProcessed += processed;
  const parseStats = collectParseStats(run);
  const warningsDelta = flushWarnings(run);

  post('PROGRESS', {
    runId,
    report,
    facility,
    page,
    lastPage,
    rowsProcessedDelta: processed,
    totalRowsProcessed: run.totalRowsProcessed,
    parseStats,
    warningsDelta,
  });

  const now = Date.now();
  const partialInterval = Math.max(PARTIAL_EMIT_INTERVAL_MS_DEFAULT, run.adaptive.partialIntervalMs);
  if (now - run.lastPartialAt >= partialInterval && !run.cancelled) {
    run.lastPartialAt = now;
    post('PARTIAL_RESULT', {
      runId,
      results: buildResults(run),
      parseStats,
      warnings: run.warnings.slice(),
      chunkSize: run.adaptive.chunkSize,
      partialIntervalMs: partialInterval,
    });
  }
}

async function handlePageBatch(data) {
  const { pages, runId } = data || {};
  if (!Array.isArray(pages) || !pages.length) return;
  const run = runs.get(runId);
  if (run) run.backlogPages += pages.length;
  for (const page of pages) {
    if (!page) continue;
    await handlePageRows({ ...page, runId });
    if (run && run.backlogPages > 0) run.backlogPages -= 1;
  }
}

function handleFinalize(data) {
  const { runId } = data;
  const run = runs.get(runId);
  if (!run || run.cancelled) {
    post('ERROR', { runId, errorCode: 'cancelled', message: 'Run was cancelled before finalize.' });
    runs.delete(runId);
    return;
  }

  const parseStats = collectParseStats(run);
  const results = buildResults(run);
  const warnings = run.warnings.slice();

  runs.delete(runId);
  post('FINAL_RESULT', {
    runId,
    results,
    warnings,
    parseStats,
    chunkSize: run.adaptive?.chunkSize || CHUNK_SIZE_DEFAULT,
    partialIntervalMs: run.adaptive?.partialIntervalMs || PARTIAL_EMIT_INTERVAL_MS_DEFAULT,
  });
}

function handleCancel(data) {
  const { runId } = data;
  const run = runs.get(runId);
  if (run) {
    run.cancelled = true;
    runs.delete(runId);
  }
  post('CANCELLED', { runId });
}

function handleReset() {
  runs.clear();
}

self.addEventListener('message', (event) => {
  const data = event.data || {};
  try {
    switch (data.type) {
      case 'INIT_RUN':
        return handleInit(data);
      case 'PAGE_ROWS':
        return handlePageRows(data);
      case 'PAGE_ROWS_BATCH':
        return handlePageBatch(data);
      case 'FINALIZE':
        return handleFinalize(data);
      case 'CANCEL':
        return handleCancel(data);
      case 'RESET':
        return handleReset();
      default:
        return;
    }
  } catch (e) {
    post('ERROR', {
      runId: data.runId,
      errorCode: 'worker_exception',
      message: 'Worker processing error',
      detail: e?.message || String(e),
    });
  }
});

post('WORKER_READY');
