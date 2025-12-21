import { createApiRunner, ApiError } from './api.js?v=2025.01.07.0';
import { createAnalyzers, normalizeRowStrict } from './analysis.js?v=2025.01.07.0';
import { renderReportResult, destroyAllCharts } from './charts.js?v=2025.01.07.0';
import { downloadText, downloadCsv, buildSummaryTxt, buildReportSummaryCsv, buildChartCsv, printReport } from './export.js?v=2025.01.07.0';
import { MOCK_TIMEZONES } from './mock-data.js?v=2025.01.07.0';
import { instrumentation } from './instrumentation.js?v=2025.01.07.0';
import { createETATracker } from './eta.js?v=2025.01.07.0';
import { createWorkerBatcher } from './worker-transfer.js?v=2025.01.07.0';
import {
  WORKER_AUTO_THRESHOLD_PAGES,
  PARTIAL_EMIT_INTERVAL_MS_DEFAULT,
  shouldAutoUseWorker,
  computeRenderThrottle,
} from './worker-adaptation.js';
import {
  PRESETS,
  getConfig,
  getConfigValue,
  setConfigValue,
  applyPreset,
  resetToDefaults,
  hasAnyOverrides,
  isOverridden,
  addChangeListener,
} from './backpressure-config.js';

const { DateTime } = window.luxon;

const PROGRESS_THROTTLE_MS = 200; // 5x/second
const PERF_RENDER_THROTTLE_MS = 900;
const PERF_DEBUG = new URLSearchParams(window.location.search).has('perf');
const WORKER_READY_TIMEOUT_MS = 1500;

const REPORTS = [
  'current_inventory',
  'detention_history',
  'dockdoor_history',
  'driver_history',
  'trailer_history'
];

const UI = {
  tenantInput: document.querySelector('#tenantInput'),
  tokenInput: document.querySelector('#tokenInput'),
  clearTokenBtn: document.querySelector('#clearTokenBtn'),
  facilitiesInput: document.querySelector('#facilitiesInput'),
  startDateInput: document.querySelector('#startDateInput'),
  endDateInput: document.querySelector('#endDateInput'),
  timezoneSelect: document.querySelector('#timezoneSelect'),
  reportChecks: Array.from(document.querySelectorAll('.reportCheck')),
  mockModeToggle: document.querySelector('#mockModeToggle'),
  runBtn: document.querySelector('#runBtn'),
  cancelBtn: document.querySelector('#cancelBtn'),
  resetAllBtn: document.querySelector('#resetAllBtn'),
  inputErrors: document.querySelector('#inputErrors'),
  progressPanel: document.querySelector('#progressPanel'),
  warningsPanel: document.querySelector('#warningsPanel'),
  resultsRoot: document.querySelector('#resultsRoot'),
  statusBanner: document.querySelector('#statusBanner'),
  downloadSummaryBtn: document.querySelector('#downloadSummaryBtn'),
  printBtn: document.querySelector('#printBtn'),
  assumptionDetention: document.querySelector('#assumptionDetention'),
  assumptionLabor: document.querySelector('#assumptionLabor'),
  assumptionTargetMoves: document.querySelector('#assumptionTargetMoves'),
  workerToggle: document.querySelector('#workerToggle'),
  workerStatus: document.querySelector('#workerStatus'),
  perfPanel: document.querySelector('#perfPanel'),
  perfCard: document.querySelector('#perfCard'),
  // Backpressure drawer elements
  bpDrawer: document.querySelector('#backpressureDrawer'),
  bpDrawerToggle: document.querySelector('#bpDrawerToggle'),
  bpDrawerClose: document.querySelector('#bpDrawerClose'),
  bpResetAll: document.querySelector('#bpResetAll'),
  bpOverrideIndicator: document.querySelector('#bpOverrideIndicator'),
  bpPresetsBody: document.querySelector('#bpPresetsBody'),
  // Backpressure controls
  bpGlobalMaxConcurrency: document.querySelector('#bpGlobalMaxConcurrency'),
  bpGreenZoneEnabled: document.querySelector('#bpGreenZoneEnabled'),
  bpGreenZoneConcurrencyMax: document.querySelector('#bpGreenZoneConcurrencyMax'),
  bpGreenZoneStreakCount: document.querySelector('#bpGreenZoneStreakCount'),
  bpFetchBufferSize: document.querySelector('#bpFetchBufferSize'),
  bpProcessingPoolSize: document.querySelector('#bpProcessingPoolSize'),
  bpPageQueueLimit: document.querySelector('#bpPageQueueLimit'),
  bpForceTier: document.querySelector('#bpForceTier'),
  bpBatchSize: document.querySelector('#bpBatchSize'),
  bpPartialUpdateInterval: document.querySelector('#bpPartialUpdateInterval'),
};

const state = {
  running: false,
  mockMode: false,
  token: null, // IMPORTANT: in-memory only
  abortController: null,
  runStartedAt: null,
  timezone: 'America/Los_Angeles',
  inputs: null,
  warnings: [],
  progress: {}, // report -> facility -> {page,lastPage,rowsProcessed,status,error}
  results: {},  // report -> result payload
  chartRegistry: new Map(), // report -> [chartHandles]
  workerPreference: true,
  resultsRenderThrottleMs: computeRenderThrottle(PARTIAL_EMIT_INTERVAL_MS_DEFAULT),
  currentRunId: null, // For cancellation correctness
  apiRunner: null, // Store runner reference for cancellation
  etaTracker: null, // ETA tracking instance
  perf: {
    enabled: PERF_DEBUG,
    startedAt: null,
    requests: new Map(), // report -> durations (ms)
    rows: 0,
    processingMs: 0,
    renderMs: 0,
    lastRender: 0,
  },
};

// Batched UI update system - coalesces multiple updates into single render
const progressRenderState = {
  pendingReports: new Set(),
  timer: null,
  renderRequested: false, // Prevents multiple renders queuing
};

const resultsRenderState = {
  timer: null,
  pending: false,
  renderRequested: false,
};

const perfRenderState = {
  timer: null,
  pending: false,
  renderRequested: false,
};

const workerRuntime = {
  supported: typeof Worker !== 'undefined' && typeof URL !== 'undefined',
  preferred: true,
  ready: false,
  fallbackReason: null,
  worker: null,
  currentRunId: null,
  finalizePromise: null,
  finalizeResolve: null,
  finalizeReject: null,
};

// ---------- Timezones ----------
function buildTimezoneOptions(selectEl) {
  const pinned = [
    { label: 'Pacific (America/Los_Angeles)', value: 'America/Los_Angeles' },
    { label: 'Mountain (America/Denver)', value: 'America/Denver' },
    { label: 'Central (America/Chicago)', value: 'America/Chicago' },
    { label: 'Eastern (America/New_York)', value: 'America/New_York' },
  ];

  // A pragmatic “longer list” without going full 400+.
  // If you want more, drop in the full IANA list later.
  const common = [
    'UTC',
    'America/Phoenix',
    'America/Anchorage',
    'America/Honolulu',
    'America/Toronto',
    'America/Vancouver',
    'America/Mexico_City',
    'America/Sao_Paulo',
    'Europe/London',
    'Europe/Dublin',
    'Europe/Paris',
    'Europe/Berlin',
    'Europe/Warsaw',
    'Europe/Athens',
    'Africa/Johannesburg',
    'Asia/Dubai',
    'Asia/Kolkata',
    'Asia/Singapore',
    'Asia/Tokyo',
    'Australia/Sydney',
    'Australia/Perth',
    'Pacific/Auckland',
  ].map(z => ({ label: z, value: z }));

  selectEl.innerHTML = '';

  const optgroupPinned = document.createElement('optgroup');
  optgroupPinned.label = 'Common (US)';
  pinned.forEach(o => optgroupPinned.appendChild(new Option(o.label, o.value)));

  const optgroupCommon = document.createElement('optgroup');
  optgroupCommon.label = 'More timezones';
  common.forEach(o => optgroupCommon.appendChild(new Option(o.label, o.value)));

  selectEl.appendChild(optgroupPinned);
  selectEl.appendChild(optgroupCommon);

  selectEl.value = state.timezone;
}

function getSelectedReports() {
  return UI.reportChecks.filter(c => c.checked).map(c => c.value);
}

function parseFacilities(text) {
  return text
    .split('\n')
    .map(s => s.trim())
    .filter(Boolean);
}

function setBanner(kind, text) {
  UI.statusBanner.classList.remove('hidden', 'callout-error', 'callout-info', 'callout-ok');
  UI.statusBanner.classList.add('callout');
  if (kind === 'error') UI.statusBanner.classList.add('callout-error');
  else if (kind === 'ok') UI.statusBanner.classList.add('callout-ok');
  else UI.statusBanner.classList.add('callout-info');
  UI.statusBanner.textContent = text;
}

function clearBanner() {
  UI.statusBanner.classList.add('hidden');
  UI.statusBanner.textContent = '';
}

function showInputError(msg) {
  UI.inputErrors.classList.remove('hidden');
  UI.inputErrors.textContent = msg;
}

function clearInputError() {
  UI.inputErrors.classList.add('hidden');
  UI.inputErrors.textContent = '';
}

// ---------- Token handling (strict) ----------
function setTokenInMemory(token) {
  // Never log the token. Never persist it.
  state.token = token || null;
}

function clearTokenEverywhere({ abort = true } = {}) {
  UI.tokenInput.value = '';
  setTokenInMemory(null);

  if (abort) {
    abortInFlight('Token cleared.');
  }
}

function abortInFlight(reason = 'Cancelled.') {
  // Invalidate runId to stop all async continuations
  state.currentRunId = null;

  if (state.abortController) {
    try { state.abortController.abort(reason); } catch {}
  }

  if (state.apiRunner) {
    try { state.apiRunner.cancel(); } catch {}
  }

  cancelWorkerRun(reason);
}

function resetAll() {
  abortInFlight('Reset all.');
  destroyAllCharts(state.chartRegistry);
  state.chartRegistry.clear();
  perfRenderState.timer && clearTimeout(perfRenderState.timer);
  perfRenderState.timer = null;
  perfRenderState.pending = false;

  state.running = false;
  state.abortController = null;
  state.runStartedAt = null;
  state.inputs = null;
  state.warnings = [];
  state.progress = {};
  state.results = {};
  state.etaTracker = null;
  resetPerfStats();
  resetWorkerState();
  flushProgressRender();
  flushResultsRender();

  clearTokenEverywhere({ abort: false });
  clearBanner();
  clearInputError();

  UI.progressPanel.innerHTML = `<div class="muted">No run yet.</div>`;
  UI.warningsPanel.textContent = 'None.';
  UI.resultsRoot.innerHTML = `
    <div class="empty-state">
      <div class="empty-title">Ready when you are.</div>
      <div class="empty-subtitle">
        Paste a token, pick facilities + reports, then run. This app streams aggregation and discards raw rows by default.
      </div>
    </div>
  `;

  UI.downloadSummaryBtn.disabled = true;
  UI.printBtn.disabled = true;
  UI.cancelBtn.disabled = true;
  UI.runBtn.disabled = false;
  if (UI.perfPanel) {
    UI.perfPanel.textContent = state.perf.enabled
      ? 'Perf debug ready. Will populate when a run starts.'
      : 'Disabled. Append ?perf=1 to enable lightweight instrumentation (no tokens/PII logged).';
  }

  // keep tenant/facilities/dates/timezone to reduce annoyance? The requirement says reset all inputs/results.
  UI.tenantInput.value = '';
  UI.facilitiesInput.value = '';
  UI.startDateInput.value = '';
  UI.endDateInput.value = '';
  UI.timezoneSelect.value = 'America/Los_Angeles';
  state.timezone = 'America/Los_Angeles';

  // ROI assumptions reset
  UI.assumptionDetention.value = '';
  UI.assumptionLabor.value = '';
  UI.assumptionTargetMoves.value = '';

  // reports reset to all checked
  UI.reportChecks.forEach(c => (c.checked = true));

  // mock mode reset OFF
  UI.mockModeToggle.checked = false;
  state.mockMode = false;
}

function setRunningUI(running) {
  state.running = running;
  UI.runBtn.disabled = running;
  UI.cancelBtn.disabled = !running;
  UI.resetAllBtn.disabled = running;
  UI.mockModeToggle.disabled = running;
  UI.downloadSummaryBtn.disabled = running || Object.keys(state.results).length === 0;
  UI.printBtn.disabled = running || Object.keys(state.results).length === 0;
  if (UI.workerToggle) {
    UI.workerToggle.disabled = running || !workerRuntime.supported || !!workerRuntime.fallbackReason;
  }
}

// ---------- Progress rendering ----------
function initProgressUI(selectedReports, facilities) {
  state.progress = {};
  UI.progressPanel.innerHTML = '';

  // Add ETA display at the top
  const etaEl = document.createElement('div');
  etaEl.id = 'etaDisplay';
  etaEl.className = 'eta-display';
  etaEl.innerHTML = `
    <div class="eta-content">
      <span class="eta-progress" data-eta-progress>Initializing...</span>
      <span class="eta-time" data-eta-time></span>
    </div>
    <div class="eta-stats" data-eta-stats></div>
  `;
  UI.progressPanel.appendChild(etaEl);

  selectedReports.forEach(report => {
    state.progress[report] = {};
    facilities.forEach(f => {
      state.progress[report][f] = { page: 0, lastPage: 0, rowsProcessed: 0, status: 'queued', error: null };
    });

    const el = document.createElement('div');
    el.className = 'progress-report';
    el.dataset.report = report;

    el.innerHTML = `
      <div class="progress-title">
        <b>${report}</b>
        <span class="progress-meta" data-meta="meta">queued</span>
      </div>
      <div class="bar" aria-hidden="true"><span data-bar="bar"></span></div>
      <div class="facilities" data-facs="facs"></div>
    `;

    const facsEl = el.querySelector('[data-facs="facs"]');
    facilities.forEach(f => {
      const pill = document.createElement('span');
      pill.className = 'fac-pill';
      pill.dataset.facility = f;
      pill.dataset.status = 'queued';
      pill.textContent = `${f}: queued`;
      facsEl.appendChild(pill);
    });

    UI.progressPanel.appendChild(el);
  });
}

function renderProgressNow(report) {
  const reportEl = UI.progressPanel.querySelector(`.progress-report[data-report="${report}"]`);
  if (!reportEl) return;

  const metaEl = reportEl.querySelector('[data-meta="meta"]');
  const barEl = reportEl.querySelector('[data-bar="bar"]');

  const facilities = Object.keys(state.progress[report] || {});
  let done = 0;
  let totalPages = 0;
  let donePages = 0;
  let anyRunning = false;
  let anyError = false;

  facilities.forEach(f => {
    const p = state.progress[report][f];
    if (p.status === 'done') done++;
    if (p.status === 'running') anyRunning = true;
    if (p.status === 'error') anyError = true;
    if (p.lastPage) totalPages += p.lastPage;
    if (p.page) donePages += p.page;

    const pill = reportEl.querySelector(`.fac-pill[data-facility="${f}"]`);
    if (pill) {
      pill.dataset.status = p.status;
      if (p.status === 'error') pill.textContent = `${f}: error`;
      else if (p.status === 'done') pill.textContent = `${f}: done`;
      else if (p.status === 'running') pill.textContent = `${f}: ${p.page || 0}/${p.lastPage || '?'} pages`;
      else pill.textContent = `${f}: ${p.status}`;
      if (p.status === 'error' && p.error) pill.title = p.error;
    }
  });

  const pct = totalPages > 0 ? Math.min(100, Math.round((donePages / totalPages) * 100)) : (done > 0 ? 100 : 0);
  barEl.style.width = `${pct}%`;

  if (anyError) metaEl.textContent = `errors detected`;
  else if (done === facilities.length && facilities.length > 0) metaEl.textContent = `complete (${donePages}/${totalPages || donePages} pages)`;
  else if (anyRunning) metaEl.textContent = `running (${donePages}/${totalPages || '?'} pages)`;
  else metaEl.textContent = `queued`;
}

function renderETANow() {
  if (!state.etaTracker) return;

  const etaEl = document.getElementById('etaDisplay');
  if (!etaEl) return;

  const estimate = state.etaTracker.getEstimate();
  const progressEl = etaEl.querySelector('[data-eta-progress]');
  const timeEl = etaEl.querySelector('[data-eta-time]');
  const statsEl = etaEl.querySelector('[data-eta-stats]');

  if (!estimate.ready) {
    progressEl.textContent = estimate.totalPages > 0
      ? `${estimate.completedPages} / ${estimate.totalPages} pages (${estimate.percentComplete}%)`
      : 'Calculating...';
    timeEl.textContent = '';
    statsEl.textContent = '';
    return;
  }

  progressEl.textContent = `${estimate.completedPages} / ${estimate.totalPages} pages (${estimate.percentComplete}%)`;
  timeEl.textContent = estimate.remainingText || '';
  timeEl.className = 'eta-time' + (estimate.remainingText ? ' eta-visible' : '');

  // Show speed stats
  const elapsedSec = Math.round(estimate.elapsedMs / 1000);
  const elapsedText = elapsedSec < 60
    ? `${elapsedSec}s elapsed`
    : `${Math.floor(elapsedSec / 60)}m ${elapsedSec % 60}s elapsed`;

  statsEl.textContent = `${estimate.pagesPerSecond} pages/sec | ${elapsedText}`;
}

function scheduleProgressRender(report) {
  progressRenderState.pendingReports.add(report);
  if (progressRenderState.renderRequested) return; // Already queued
  progressRenderState.renderRequested = true;

  if (progressRenderState.timer) return;
  progressRenderState.timer = setTimeout(() => {
    const toRender = Array.from(progressRenderState.pendingReports);
    progressRenderState.pendingReports.clear();
    progressRenderState.timer = null;
    progressRenderState.renderRequested = false;
    instrumentation.recordBatchedUpdate();
    toRender.forEach(renderProgressNow);
    renderETANow();
  }, PROGRESS_THROTTLE_MS);
}

function flushProgressRender() {
  if (progressRenderState.timer) {
    clearTimeout(progressRenderState.timer);
    progressRenderState.timer = null;
  }
  const toRender = Array.from(progressRenderState.pendingReports);
  progressRenderState.pendingReports.clear();
  toRender.forEach(renderProgressNow);
}

function addWarning(msg) {
  const stamp = DateTime.now().setZone(state.timezone).toFormat('yyyy-LL-dd HH:mm:ss');
  state.warnings.push(`[${stamp}] ${msg}`);
  UI.warningsPanel.textContent = state.warnings.length ? state.warnings.join('\n') : 'None.';
}

// ---------- Perf instrumentation (lightweight, optional) ----------
function resetPerfStats() {
  if (!state.perf.enabled) return;
  state.perf.startedAt = performance.now();
  state.perf.requests = new Map();
  state.perf.rows = 0;
  state.perf.processingMs = 0;
  state.perf.renderMs = 0;
  state.perf.lastRender = 0;
  renderPerfNow();
}

function quantiles(arr) {
  if (!arr.length) return { avg: 0, p50: 0, p90: 0 };
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (p) => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] || 0;
  const sum = sorted.reduce((a, b) => a + b, 0);
  return { avg: sum / sorted.length, p50: idx(0.5), p90: idx(0.9) };
}

function renderPerfNow() {
  if (!state.perf.enabled || !UI.perfPanel) return;
  const now = performance.now();
  state.perf.lastRender = now;
  const elapsedMs = state.perf.startedAt ? now - state.perf.startedAt : 0;
  const elapsedSec = elapsedMs / 1000;

  const lines = [];
  lines.push(`Runtime: ${elapsedSec ? elapsedSec.toFixed(1) : '0.0'}s`);
  lines.push(`Rows processed: ${state.perf.rows} (${elapsedSec ? Math.round((state.perf.rows / elapsedSec) * 10) / 10 : 0} rows/sec)`);
  if (state.perf.processingMs) {
    const perRow = state.perf.rows ? (state.perf.processingMs / state.perf.rows) : 0;
    lines.push(`Processing time: ${Math.round(state.perf.processingMs)} ms (avg ${perRow.toFixed(3)} ms/row)`);
  }
  if (state.perf.renderMs) {
    lines.push(`Chart render time: ${Math.round(state.perf.renderMs)} ms (throttled)`);
  }

  if (state.perf.requests?.size) {
    lines.push('Request latency (per report):');
    for (const [report, arr] of Array.from(state.perf.requests.entries()).sort()) {
      const stats = quantiles(arr);
      lines.push(` • ${report}: avg ${Math.round(stats.avg)}ms · p50 ${Math.round(stats.p50)}ms · p90 ${Math.round(stats.p90)}ms`);
    }
  }

  UI.perfPanel.textContent = lines.join('\n');
  UI.perfPanel.classList.remove('muted');
}

function schedulePerfRender() {
  if (!state.perf.enabled || !UI.perfPanel) return;
  if (perfRenderState.timer) {
    perfRenderState.pending = true;
    return;
  }
  perfRenderState.timer = setTimeout(() => {
    perfRenderState.timer = null;
    perfRenderState.pending = false;
    renderPerfNow();
  }, PERF_RENDER_THROTTLE_MS);
}

function recordRequestTiming({ report, ms }) {
  if (!state.perf.enabled) return;
  if (!state.perf.requests.has(report)) state.perf.requests.set(report, []);
  const arr = state.perf.requests.get(report);
  arr.push(ms);
  if (arr.length > 200) arr.shift();
  schedulePerfRender();
}

function recordProcessing({ rows, ms }) {
  if (!state.perf.enabled) return;
  state.perf.rows += rows;
  state.perf.processingMs += ms;
  schedulePerfRender();
}

function recordRender(ms) {
  if (!state.perf.enabled) return;
  state.perf.renderMs += ms;
  schedulePerfRender();
}

// ---------- Main-thread ingestion helper (yields to keep UI responsive) ----------
const MAIN_THREAD_INGEST_CHUNK = 200;  // Reduced chunk size for better responsiveness

async function ingestRowsChunked({ rows, report, timezone, analyzer, onWarning, signal }) {
  const incoming = Array.isArray(rows) ? rows : [];
  let processed = 0;
  const t0 = state.perf.enabled ? performance.now() : 0;

  for (let start = 0; start < incoming.length; start += MAIN_THREAD_INGEST_CHUNK) {
    if (signal?.aborted) break;
    const end = Math.min(incoming.length, start + MAIN_THREAD_INGEST_CHUNK);
    for (let i = start; i < end; i++) {
      const normalized = normalizeRowStrict(incoming[i], { report, timezone, onWarning });
      if (normalized) {
        analyzer.ingest(normalized);
        processed++;
      }
    }
    // Always yield after each chunk to keep UI responsive
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  const dt = state.perf.enabled ? (performance.now() - t0) : 0;
  return { processed, durationMs: dt };
}

function createMainThreadIngestQueue({ analyzers, timezone, onWarning, signal }) {
  // Use a bounded queue instead of unbounded array
  const MAX_PENDING = 8;  // Max pending ingestion tasks
  let pendingCount = 0;
  let completedCount = 0;
  let currentChain = Promise.resolve();
  let drainResolve = null;
  let drainPromise = null;

  const checkDrain = () => {
    if (drainResolve && pendingCount === 0) {
      drainResolve();
      drainResolve = null;
      drainPromise = null;
    }
  };

  const enqueue = async ({ report, rows }) => {
    const analyzer = analyzers?.[report];
    if (!analyzer) return null;

    // Apply backpressure if too many pending
    while (pendingCount >= MAX_PENDING) {
      await new Promise(resolve => setTimeout(resolve, 10));
      if (signal?.aborted) return null;
    }

    pendingCount++;

    // Chain tasks to ensure sequential processing per enqueue call
    const task = currentChain.then(async () => {
      if (signal?.aborted) {
        pendingCount--;
        checkDrain();
        return { processed: 0, durationMs: 0 };
      }
      try {
        return await ingestRowsChunked({ rows, report, timezone, analyzer, onWarning, signal });
      } finally {
        pendingCount--;
        completedCount++;
        checkDrain();
      }
    });

    // Update chain but don't hold reference to old chain
    currentChain = task.catch(() => {});

    return task;
  };

  const drain = () => {
    if (pendingCount === 0) return Promise.resolve();
    if (!drainPromise) {
      drainPromise = new Promise(resolve => {
        drainResolve = resolve;
      });
    }
    return drainPromise;
  };

  return { enqueue, drain };
}

// ---------- Worker helpers ----------
function updateWorkerStatus(text) {
  if (UI.workerStatus) UI.workerStatus.textContent = text;
  if (UI.workerToggle) {
    UI.workerToggle.disabled = !workerRuntime.supported || !!workerRuntime.fallbackReason || state.running;
    UI.workerToggle.checked = workerRuntime.supported && !workerRuntime.fallbackReason && workerRuntime.preferred;
  }
}

async function waitForWorkerReady(timeoutMs = WORKER_READY_TIMEOUT_MS) {
  if (!workerRuntime.supported) return false;
  if (workerRuntime.ready) return true;
  const start = performance.now ? performance.now() : Date.now();
  while ((performance.now ? performance.now() : Date.now()) - start < timeoutMs) {
    if (workerRuntime.ready) return true;
    await new Promise(resolve => setTimeout(resolve, 30));
  }
  workerRuntime.fallbackReason = 'Worker handshake timeout';
  updateWorkerStatus('Worker handshake timeout; using main thread.');
  return false;
}

function handleWorkerMessage(event) {
  const data = event.data || {};
  switch (data.type) {
    case 'WORKER_READY':
      workerRuntime.ready = true;
      workerRuntime.fallbackReason = null;
      updateWorkerStatus('Web Worker ready (keeps UI responsive).');
      break;
    case 'PROGRESS':
      if (data.runId !== workerRuntime.currentRunId) return;
      if (Array.isArray(data.warningsDelta)) data.warningsDelta.forEach(addWarning);
      if (state.progress?.[data.report]?.[data.facility]) {
        state.progress[data.report][data.facility].rowsProcessed = data.totalRowsProcessed;
        scheduleProgressRender(data.report);
      }
      break;
    case 'PARTIAL_RESULT':
      if (data.runId !== workerRuntime.currentRunId) return;
      if (data.results && state.running) {
        state.results = data.results;
        if (data.partialIntervalMs) {
          state.resultsRenderThrottleMs = computeRenderThrottle(data.partialIntervalMs);
        }
        scheduleResultsRender();
      }
      break;
    case 'FINAL_RESULT':
      if (data.runId !== workerRuntime.currentRunId) return;
      workerRuntime.currentRunId = null;
      workerRuntime.finalizeResolve?.(data);
      workerRuntime.finalizePromise = null;
      workerRuntime.finalizeResolve = null;
      workerRuntime.finalizeReject = null;
      break;
    case 'CANCELLED':
      if (data.runId !== workerRuntime.currentRunId) return;
      workerRuntime.currentRunId = null;
      workerRuntime.finalizeReject?.(new Error('Worker cancelled'));
      workerRuntime.finalizePromise = null;
      workerRuntime.finalizeResolve = null;
      workerRuntime.finalizeReject = null;
      break;
    case 'ERROR':
      addWarning(`Worker error: ${data.message || data.errorCode || 'unknown'}`);
      if (data.runId && data.runId === workerRuntime.currentRunId) {
        workerRuntime.finalizeReject?.(new Error(data.message || 'Worker error'));
        workerRuntime.currentRunId = null;
        workerRuntime.finalizePromise = null;
        workerRuntime.finalizeResolve = null;
        workerRuntime.finalizeReject = null;
      }
      workerRuntime.fallbackReason = data.message || data.errorCode || 'worker error';
      workerRuntime.ready = false;
      updateWorkerStatus('Worker unavailable; falling back to main thread.');
      break;
    default:
      break;
  }
}

function initWorker() {
  if (!workerRuntime.supported) {
    workerRuntime.fallbackReason = 'Web Worker not supported';
    workerRuntime.preferred = false;
    updateWorkerStatus('Web Worker unavailable in this browser; using main thread.');
    return;
  }

  try {
    workerRuntime.worker = new Worker(new URL('./analysis.worker.js', import.meta.url), { type: 'module' });
    workerRuntime.worker.onmessage = handleWorkerMessage;
    workerRuntime.worker.onerror = (err) => {
      addWarning(`Worker error: ${err?.message || err}`);
      workerRuntime.ready = false;
      workerRuntime.fallbackReason = err?.message || 'Worker error';
      updateWorkerStatus('Worker unavailable; falling back to main thread.');
    };
  } catch (e) {
    workerRuntime.fallbackReason = e?.message || 'Worker initialization failed';
    workerRuntime.ready = false;
    workerRuntime.preferred = false;
    updateWorkerStatus('Worker initialization failed; using main thread.');
  }
}

function shouldUseWorker() {
  return workerRuntime.supported && workerRuntime.preferred && workerRuntime.ready && !workerRuntime.fallbackReason;
}

function beginWorkerRun(config) {
  if (!shouldUseWorker() || !workerRuntime.worker) return null;

  const runId = `run_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  workerRuntime.currentRunId = runId;
  workerRuntime.finalizePromise = new Promise((resolve, reject) => {
    workerRuntime.finalizeResolve = resolve;
    workerRuntime.finalizeReject = reject;
  });

  workerRuntime.worker.postMessage({ type: 'INIT_RUN', runId, ...config });
  return { runId, finalizePromise: workerRuntime.finalizePromise };
}

function finalizeWorkerRun(runId) {
  if (!runId || !workerRuntime.worker || !workerRuntime.finalizePromise) return Promise.reject(new Error('Worker not ready'));
  workerRuntime.worker.postMessage({ type: 'FINALIZE', runId });
  return workerRuntime.finalizePromise;
}

function cancelWorkerRun(reason = 'cancelled') {
  if (workerRuntime.currentRunId && workerRuntime.worker) {
    workerRuntime.worker.postMessage({ type: 'CANCEL', runId: workerRuntime.currentRunId });
  }
  workerRuntime.currentRunId = null;
  if (workerRuntime.finalizeReject) workerRuntime.finalizeReject(new Error(reason));
  workerRuntime.finalizePromise = null;
  workerRuntime.finalizeResolve = null;
  workerRuntime.finalizeReject = null;
}

function resetWorkerState() {
  if (workerRuntime.worker) {
    workerRuntime.worker.postMessage({ type: 'RESET' });
  }
  workerRuntime.currentRunId = null;
  workerRuntime.finalizePromise = null;
  workerRuntime.finalizeResolve = null;
  workerRuntime.finalizeReject = null;
}

// ---------- Results rendering ----------
function renderAllResults() {
  const renderStart = state.perf.enabled ? performance.now() : 0;
  destroyAllCharts(state.chartRegistry);
  state.chartRegistry.clear();

  const inputs = state.inputs;
  const reports = Object.keys(state.results);

  if (!reports.length) {
    UI.resultsRoot.innerHTML = `
      <div class="empty-state">
        <div class="empty-title">No results yet.</div>
        <div class="empty-subtitle">Run an assessment to see metrics, charts, findings, and exports.</div>
      </div>
    `;
    return;
  }

  const runAt = state.runStartedAt
    ? DateTime.fromMillis(state.runStartedAt).setZone(inputs.timezone).toFormat('yyyy-LL-dd HH:mm')
    : '—';

  const root = document.createElement('div');

  const summary = document.createElement('div');
  summary.className = 'report-card';
  summary.innerHTML = `
    <div class="section-title">
      <h2>Assessment summary</h2>
      <span class="badge ${state.mockMode ? 'yellow' : 'green'}">${state.mockMode ? 'Mock mode' : 'Live API'}</span>
    </div>

    <div class="kpi-grid">
      <div class="kpi">
        <div class="label">Tenant</div>
        <div class="value">${escapeHtml(inputs.tenant)}</div>
        <div class="sub">Timezone: <b>${escapeHtml(inputs.timezone)}</b></div>
      </div>
      <div class="kpi">
        <div class="label">Facilities</div>
        <div class="value">${inputs.facilities.length}</div>
        <div class="sub">${inputs.facilities.map(escapeHtml).join(', ')}</div>
      </div>
      <div class="kpi">
        <div class="label">Date range</div>
        <div class="value">${escapeHtml(inputs.startDate)} → ${escapeHtml(inputs.endDate)}</div>
        <div class="sub">Run started: ${escapeHtml(runAt)}</div>
      </div>
    </div>

    <div class="muted small" style="margin-top:10px;">
      Raw rows are not retained by default; charts and exports are based on aggregated series/tables only.
      Fields containing <code>cell</code> or <code>phone</code> are scrubbed during normalization.
    </div>
  `;
  root.appendChild(summary);

  reports.forEach(report => {
    const result = state.results[report];
    const card = renderReportResult({
      report,
      result,
      timezone: inputs.timezone,
      dateRange: { startDate: inputs.startDate, endDate: inputs.endDate },
      onDownloadChartPng: ({ filename, dataUrl }) => {
        // handled in charts.js button wiring; keeping hook for future
      },
      onDownloadChartCsv: ({ filename, csvText }) => {
        downloadText(filename, csvText);
      },
      onDownloadReportCsv: ({ filename, csvText }) => {
        downloadText(filename, csvText);
      },
      onWarning: addWarning,
      chartRegistry: state.chartRegistry,
    });
    root.appendChild(card);
  });

  UI.resultsRoot.innerHTML = '';
  UI.resultsRoot.appendChild(root);

  UI.downloadSummaryBtn.disabled = false;
  UI.printBtn.disabled = false;
  if (state.perf.enabled) {
    recordRender(performance.now() - renderStart);
  }
}

function scheduleResultsRender() {
  if (resultsRenderState.renderRequested) return; // Already queued
  resultsRenderState.renderRequested = true;

  if (resultsRenderState.timer) {
    resultsRenderState.pending = true;
    return;
  }
  const throttleMs = state.resultsRenderThrottleMs || computeRenderThrottle(PARTIAL_EMIT_INTERVAL_MS_DEFAULT);
  resultsRenderState.timer = setTimeout(() => {
    resultsRenderState.timer = null;
    resultsRenderState.pending = false;
    resultsRenderState.renderRequested = false;
    instrumentation.recordBatchedUpdate();
    const renderStart = performance.now();
    renderAllResults();
    instrumentation.recordRender(performance.now() - renderStart);
  }, CHART_RENDER_THROTTLE_MS);
}

function flushResultsRender() {
  if (resultsRenderState.timer) {
    clearTimeout(resultsRenderState.timer);
    resultsRenderState.timer = null;
  }
  resultsRenderState.pending = false;
  renderAllResults();
}

function escapeHtml(s) {
  return String(s ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

// ---------- Validation ----------
function validateInputs(inputs) {
  if (!inputs.tenant || !/^[a-z0-9-]+$/i.test(inputs.tenant)) {
    return 'Tenant must be a non-empty subdomain (letters/numbers/dash).';
  }
  if (!state.mockMode) {
    if (!inputs.token || inputs.token.length < 10) return 'Token is required for live API mode.';
  }
  if (!inputs.facilities.length) return 'At least one facility code is required.';
  if (!inputs.reports.length) return 'Select at least one report.';
  if (!inputs.startDate || !inputs.endDate) return 'Start and end dates are required.';
  if (inputs.startDate > inputs.endDate) return 'Start date must be <= end date.';
  if (!inputs.timezone) return 'Timezone is required.';
  return null;
}

function readAssumptions() {
  const detention = UI.assumptionDetention.value.trim();
  const labor = UI.assumptionLabor.value.trim();
  const targetMoves = UI.assumptionTargetMoves.value.trim();

  return {
    detention_cost_per_hour: detention ? Number(detention) : null,
    labor_fully_loaded_rate_per_hour: labor ? Number(labor) : null,
    target_moves_per_driver_per_day: targetMoves ? Number(targetMoves) : 50,
  };
}

function canComputeROI(a) {
  return (
    Number.isFinite(a.detention_cost_per_hour) &&
    Number.isFinite(a.labor_fully_loaded_rate_per_hour) &&
    Number.isFinite(a.target_moves_per_driver_per_day)
  );
}

// ---------- Run assessment ----------
async function runAssessment() {
  clearInputError();
  clearBanner();
  state.warnings = [];
  UI.warningsPanel.textContent = 'None.';

  const inputs = {
    tenant: UI.tenantInput.value.trim(),
    token: UI.tokenInput.value, // read once; then keep in-memory only during run
    facilities: parseFacilities(UI.facilitiesInput.value),
    startDate: UI.startDateInput.value,
    endDate: UI.endDateInput.value,
    timezone: UI.timezoneSelect.value,
    reports: getSelectedReports(),
    assumptions: readAssumptions(),
    mockMode: state.mockMode,
  };

  const err = validateInputs(inputs);
  if (err) {
    showInputError(err);
    return;
  }

  // Put token in memory *only for this run*, then wipe on completion/cancel.
  setTokenInMemory(inputs.token);
  resetPerfStats();
  instrumentation.reset();

  state.inputs = {
    tenant: inputs.tenant,
    facilities: inputs.facilities,
    startDate: inputs.startDate,
    endDate: inputs.endDate,
    timezone: inputs.timezone,
    reports: inputs.reports,
    assumptions: inputs.assumptions,
    mockMode: inputs.mockMode,
  };

  destroyAllCharts(state.chartRegistry);
  state.chartRegistry.clear();
  state.results = {};
  state.progress = {};
  state.runStartedAt = Date.now();
  state.timezone = inputs.timezone;
  state.resultsRenderThrottleMs = computeRenderThrottle(PARTIAL_EMIT_INTERVAL_MS_DEFAULT);

  // Initialize ETA tracker
  state.etaTracker = createETATracker();
  state.etaTracker.start();

  initProgressUI(inputs.reports, inputs.facilities);
  setRunningUI(true);
  setBanner('info', 'Running assessment… streaming pages, updating metrics, discarding raw rows.');
  flushProgressRender();
  flushResultsRender();

  state.abortController = new AbortController();
  const signal = state.abortController.signal;

  // Generate unique runId for this assessment
  const assessmentRunId = `assess_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  state.currentRunId = assessmentRunId;

  const roiEnabled = canComputeROI(inputs.assumptions);

  let workerRun = null;
  let workerBatcher = null;
  let analyzers = null;
  let mainThreadIngest = null;
  let analysisMode = null; // 'worker' | 'main'
  let analysisInitPromise = null;
  const previousWorkerPreference = state.workerPreference;
  let autoPreferenceOverride = false;

  const ensureAnalysisMode = async ({ lastPage, sampleRowCount }) => {
    if (analysisMode) return analysisMode;
    if (analysisInitPromise) return analysisInitPromise;
    analysisInitPromise = (async () => {
      const estimatedPages = typeof lastPage === 'number' && lastPage > 0
        ? lastPage
        : (sampleRowCount > 0 ? Math.ceil((WORKER_AUTO_THRESHOLD_PAGES * 50) / sampleRowCount) : null);
      const workerAvailable = workerRuntime.supported && !workerRuntime.fallbackReason;
      const preferWorker = state.workerPreference;
      const autoWorker = shouldAutoUseWorker({
        estimatedPages,
        threshold: WORKER_AUTO_THRESHOLD_PAGES,
        workerAvailable,
        preferred: preferWorker,
      });

      if (autoWorker && workerAvailable) {
        if (!preferWorker) {
          addWarning(`Large dataset detected (${estimatedPages || 'many'} pages). Auto-enabling Web Worker to protect UI responsiveness.`);
          autoPreferenceOverride = true;
          workerRuntime.preferred = true;
          state.workerPreference = true;
        }
        const ready = await waitForWorkerReady();
        if (ready) {
          // Get current backpressure config for worker settings
          const bpConfig = getConfig();
          workerRun = beginWorkerRun({
            timezone: inputs.timezone,
            startDate: inputs.startDate,
            endDate: inputs.endDate,
            assumptions: inputs.assumptions,
            selectedReports: inputs.reports,
            facilities: inputs.facilities,
            tenant: inputs.tenant,
            roiEnabled,
            partialEmitIntervalMs: bpConfig.partialUpdateInterval,
          });
          if (workerRun && workerRuntime.worker) {
            workerBatcher = createWorkerBatcher({
              runId: workerRun.runId,
              signal,
              postMessage: (payload) => workerRuntime.worker?.postMessage(payload),
              maxBatchRows: bpConfig.batchSize,
            });
            analysisMode = 'worker';
            updateWorkerStatus('Web Worker active for this run.');
            return analysisMode;
          }
        }
        addWarning('Web Worker unavailable; using main-thread analysis.');
      }

      analyzers = createAnalyzers({
        timezone: inputs.timezone,
        startDate: inputs.startDate,
        endDate: inputs.endDate,
        assumptions: inputs.assumptions,
        onWarning: (w) => addWarning(w),
      });
      mainThreadIngest = createMainThreadIngestQueue({
        analyzers,
        timezone: inputs.timezone,
        onWarning: addWarning,
        signal,
      });
      analysisMode = 'main';
      return analysisMode;
    })();
    return analysisInitPromise;
  };

  const apiRunner = createApiRunner({
    tenant: inputs.tenant,
    tokenGetter: () => state.token, // token remains in-memory only
    mockMode: state.mockMode,
    signal,
    onProgress: ({ report, facility, page, lastPage, rowsProcessed }) => {
      if (state.currentRunId !== assessmentRunId) return; // Cancelled
      const p = state.progress?.[report]?.[facility];
      if (!p) return;
      p.status = 'running';
      p.page = page;
      p.lastPage = lastPage;
      p.rowsProcessed = rowsProcessed;

      // Update ETA tracker
      if (state.etaTracker) {
        state.etaTracker.setTotalPages(report, facility, lastPage);
        state.etaTracker.recordPageComplete(report, facility);
      }

      scheduleProgressRender(report);
    },
    onFacilityStatus: ({ report, facility, status, error }) => {
      if (state.currentRunId !== assessmentRunId) return; // Cancelled
      const p = state.progress?.[report]?.[facility];
      if (!p) return;
      p.status = status;
      p.error = error || null;
      scheduleProgressRender(report);
    },
    onWarning: addWarning,
    onAdaptiveChange: ({ direction, concurrency }) => {
      if (PERF_DEBUG) addWarning(`Adaptive concurrency ${direction === 'up' ? 'increased' : 'reduced'} to ${concurrency}.`);
    },
    onLaneChange: ({ report, direction, limit, reason }) => {
      if (PERF_DEBUG) addWarning(`Lane ${report} ${direction === 'up' ? 'recovered' : 'backed off'} to ${limit} (reason: ${reason || 'transient'}).`);
    },
    onPerf: (payload) => {
      if (payload?.type === 'request') recordRequestTiming(payload);
    },
  });
  state.apiRunner = apiRunner; // Store for cancellation

  // Run each report/facility streaming -> analyzer
  try {
    await apiRunner.run({
      reports: inputs.reports,
      facilities: inputs.facilities,
      startDate: inputs.startDate,
      endDate: inputs.endDate,
      timezone: inputs.timezone,
      onRows: async ({ report, facility, page, lastPage, rows, runId: pageRunId }) => {
        // Check cancellation via runId
        if (state.currentRunId !== assessmentRunId) return;
        if (pageRunId && pageRunId !== apiRunner.runId) return;

        const mode = await ensureAnalysisMode({ lastPage, sampleRowCount: Array.isArray(rows) ? rows.length : 0 });

        if (mode === 'worker' && workerBatcher) {
          await workerBatcher.enqueue({ report, facility, page, lastPage, rows });
          return;
        }

        const analyzer = analyzers?.[report];
        if (!analyzer) return;

        // Stream/aggregate pattern: process rows immediately and discard page data
        const task = mainThreadIngest?.enqueue({ report, rows });
        if (task) {
          try {
            const parseStart = performance.now();
            const result = await task;
            const parseTime = performance.now() - parseStart;
            instrumentation.recordParse(parseTime);

            if (state.perf.enabled && result) {
              const { processed, durationMs } = result;
              if (durationMs != null) {
                recordProcessing({ rows: processed, ms: durationMs });
                instrumentation.recordAnalysis(durationMs);
              }
            }
          } catch (err) {
            // Ignore ingestion errors; continue with next page
          }
        }
        // Page data (rows) is now eligible for GC - not retained
      }
    });

    // Finalize per report
    if (!analysisMode) {
      await ensureAnalysisMode({ lastPage: 0, sampleRowCount: 0 });
    }

    if (analysisMode === 'worker' && workerRun) {
      await workerBatcher?.flush();
      const payload = await finalizeWorkerRun(workerRun.runId);
      if (Array.isArray(payload?.warnings)) payload.warnings.forEach(addWarning);
      state.results = payload?.results || {};
      if (payload?.partialIntervalMs) {
        state.resultsRenderThrottleMs = computeRenderThrottle(payload.partialIntervalMs);
      }
    } else {
      await mainThreadIngest?.drain();
      for (const report of inputs.reports) {
        const analyzer = analyzers?.[report];
        if (!analyzer) continue;
        state.results[report] = analyzer.finalize({
          tenant: inputs.tenant,
          facilities: inputs.facilities,
          startDate: inputs.startDate,
          endDate: inputs.endDate,
          timezone: inputs.timezone,
          assumptions: inputs.assumptions,
          roiEnabled,
        });
      }
    }

    flushProgressRender();
    flushResultsRender();
    setBanner('ok', 'Complete. You can export summaries, print, and download chart CSV/PNG.');

  } catch (e) {
    if (analysisMode === 'worker' && workerRun) cancelWorkerRun('Run failed');
    if (signal.aborted) {
      setBanner('info', 'Run cancelled.');
    } else if (e instanceof ApiError) {
      setBanner('error', `API error: ${e.message}`);
      addWarning(`API error: ${e.message}`);
      if (e.status === 401 || e.status === 403) {
        abortInFlight('Unauthorized (401/403)');
      }
    } else {
      setBanner('error', `Unexpected error: ${e?.message || String(e)}`);
      addWarning(`Unexpected error: ${e?.stack || e?.message || String(e)}`);
    }
  } finally {
    await workerBatcher?.stop?.();
    flushProgressRender();
    flushResultsRender();
    // Critical: null out token from memory (but leave it in the input field for user convenience)
    setTokenInMemory(null);
    state.abortController = null;
    if (autoPreferenceOverride) {
      workerRuntime.preferred = previousWorkerPreference;
      state.workerPreference = previousWorkerPreference;
      updateWorkerStatus(previousWorkerPreference
        ? 'Web Worker enabled when available.'
        : 'Web Worker disabled; analysis will run on the main thread.');
    }
    setRunningUI(false);
  }
}

// ---------- Export wiring ----------
function downloadSummary() {
  if (!state.inputs || !Object.keys(state.results).length) return;
  const txt = buildSummaryTxt({
    inputs: state.inputs,
    results: state.results,
    warnings: state.warnings,
  });
  const stamp = DateTime.now().setZone(state.inputs.timezone).toFormat('yyyyLLdd_HHmm');
  downloadText(`YMS_Value_Assessment_${state.inputs.tenant}_${stamp}.txt`, txt);
}

function doPrint() {
  if (!state.inputs || !Object.keys(state.results).length) return;
  printReport();
}

// ---------- Events ----------
UI.runBtn.addEventListener('click', runAssessment);

UI.cancelBtn.addEventListener('click', () => {
  abortInFlight('User cancelled.');
  // Token cleared from memory but kept in input field for convenience
  setTokenInMemory(null);
});

UI.clearTokenBtn.addEventListener('click', () => {
  clearTokenEverywhere({ abort: true });
  setBanner('info', 'Token cleared (memory + input) and in-flight requests aborted.');
});

UI.resetAllBtn.addEventListener('click', resetAll);

UI.mockModeToggle.addEventListener('change', () => {
  state.mockMode = UI.mockModeToggle.checked;
  setBanner('info', state.mockMode
    ? 'Mock mode enabled. No network requests will be made.'
    : 'Mock mode disabled. Live API requests will be used on next run.'
  );

  // In mock mode, we allow empty tenant/token for demos,
  // but we still validate tenant format if provided.
});

UI.timezoneSelect.addEventListener('change', () => {
  state.timezone = UI.timezoneSelect.value;
});

UI.downloadSummaryBtn.addEventListener('click', downloadSummary);
UI.printBtn.addEventListener('click', doPrint);

UI.workerToggle?.addEventListener('change', () => {
  workerRuntime.preferred = UI.workerToggle.checked;
  state.workerPreference = workerRuntime.preferred;
  updateWorkerStatus(workerRuntime.preferred
    ? 'Web Worker enabled when available.'
    : 'Web Worker disabled; analysis will run on the main thread.'
  );
});

// Safety: do not log tokens even in debug. (No debug console in this draft.)

// ---------- Backpressure Drawer ----------

// Map of control IDs to config keys
const BP_CONTROL_MAP = {
  bpGlobalMaxConcurrency: 'globalMaxConcurrency',
  bpGreenZoneEnabled: 'greenZoneEnabled',
  bpGreenZoneConcurrencyMax: 'greenZoneConcurrencyMax',
  bpGreenZoneStreakCount: 'greenZoneStreakCount',
  bpFetchBufferSize: 'fetchBufferSize',
  bpProcessingPoolSize: 'processingPoolSize',
  bpPageQueueLimit: 'pageQueueLimit',
  bpForceTier: 'forceTier',
  bpBatchSize: 'batchSize',
  bpPartialUpdateInterval: 'partialUpdateInterval',
};

function openBackpressureDrawer() {
  UI.bpDrawer?.classList.add('open');
  UI.bpDrawerToggle?.classList.add('hidden');
}

function closeBackpressureDrawer() {
  UI.bpDrawer?.classList.remove('open');
  UI.bpDrawerToggle?.classList.remove('hidden');
}

function updateBpOverrideIndicator() {
  if (UI.bpOverrideIndicator) {
    UI.bpOverrideIndicator.classList.toggle('hidden', !hasAnyOverrides());
  }
}

function updateBpValueDisplay(controlId, value) {
  const valueEl = document.querySelector(`.bp-value[data-for="${controlId}"]`);
  if (valueEl) {
    valueEl.textContent = value;
    const configKey = BP_CONTROL_MAP[controlId];
    valueEl.classList.toggle('modified', configKey && isOverridden(configKey));
  }
}

function updateBpControlFromConfig(controlId, configKey) {
  const el = UI[controlId];
  if (!el) return;

  const value = getConfigValue(configKey);

  if (el.type === 'checkbox') {
    el.checked = value;
    // Update dependent controls
    const dependents = document.querySelectorAll(`[data-depends-on="${controlId}"]`);
    dependents.forEach(dep => {
      dep.classList.toggle('disabled', !value);
    });
  } else if (el.tagName === 'SELECT') {
    el.value = value;
  } else if (el.type === 'range') {
    el.value = value;
    updateBpValueDisplay(controlId, value);
  }
}

function syncAllBpControlsFromConfig() {
  for (const [controlId, configKey] of Object.entries(BP_CONTROL_MAP)) {
    updateBpControlFromConfig(controlId, configKey);
  }
  updateBpOverrideIndicator();
}

function initBackpressurePresets() {
  if (!UI.bpPresetsBody) return;

  UI.bpPresetsBody.innerHTML = '';

  for (const [presetId, preset] of Object.entries(PRESETS)) {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td class="bp-preset-name">${escapeHtml(preset.name)}</td>
      <td class="bp-preset-desc">${escapeHtml(preset.description)}</td>
      <td>
        <button class="btn btn-ghost bp-preset-btn" data-preset="${presetId}">Apply</button>
      </td>
    `;
    UI.bpPresetsBody.appendChild(row);
  }

  // Add event listeners to preset buttons
  UI.bpPresetsBody.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-preset]');
    if (btn) {
      const presetId = btn.dataset.preset;
      applyPreset(presetId);
      syncAllBpControlsFromConfig();

      // Brief visual feedback
      btn.textContent = 'Applied!';
      setTimeout(() => { btn.textContent = 'Apply'; }, 800);
    }
  });
}

function initBackpressureDrawer() {
  // Drawer open/close
  UI.bpDrawerToggle?.addEventListener('click', openBackpressureDrawer);
  UI.bpDrawerClose?.addEventListener('click', closeBackpressureDrawer);

  // Close drawer on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && UI.bpDrawer?.classList.contains('open')) {
      closeBackpressureDrawer();
    }
  });

  // Reset button
  UI.bpResetAll?.addEventListener('click', () => {
    resetToDefaults();
    syncAllBpControlsFromConfig();
  });

  // Initialize presets table
  initBackpressurePresets();

  // Set up slider controls
  const sliderControls = [
    'bpGlobalMaxConcurrency',
    'bpGreenZoneConcurrencyMax',
    'bpGreenZoneStreakCount',
    'bpFetchBufferSize',
    'bpProcessingPoolSize',
    'bpPageQueueLimit',
    'bpBatchSize',
    'bpPartialUpdateInterval',
  ];

  sliderControls.forEach(controlId => {
    const el = UI[controlId];
    if (!el) return;

    el.addEventListener('input', () => {
      const value = Number(el.value);
      updateBpValueDisplay(controlId, value);
    });

    el.addEventListener('change', () => {
      const configKey = BP_CONTROL_MAP[controlId];
      if (configKey) {
        setConfigValue(configKey, Number(el.value));
        updateBpOverrideIndicator();
      }
    });
  });

  // Green Zone toggle
  UI.bpGreenZoneEnabled?.addEventListener('change', () => {
    const enabled = UI.bpGreenZoneEnabled.checked;
    setConfigValue('greenZoneEnabled', enabled);

    // Update dependent controls
    const dependents = document.querySelectorAll('[data-depends-on="bpGreenZoneEnabled"]');
    dependents.forEach(dep => {
      dep.classList.toggle('disabled', !enabled);
    });

    updateBpOverrideIndicator();
  });

  // Tier select
  UI.bpForceTier?.addEventListener('change', () => {
    setConfigValue('forceTier', UI.bpForceTier.value);
    updateBpOverrideIndicator();
  });

  // Listen for config changes (e.g., from presets) to update UI
  addChangeListener(() => {
    updateBpOverrideIndicator();
  });

  // Initialize all controls from current config (defaults)
  syncAllBpControlsFromConfig();
}

// ---------- Init ----------
(function init() {
  buildTimezoneOptions(UI.timezoneSelect);

  if (UI.perfPanel) {
    UI.perfPanel.textContent = state.perf.enabled
      ? 'Perf debug ready. Enable via ?perf=1 (already on) to view live timings.'
      : 'Disabled. Append ?perf=1 to enable lightweight instrumentation (no tokens/PII logged).';
  }

  // default dates: last 90 days
  const tz = state.timezone;
  const end = DateTime.now().setZone(tz).toISODate();
  const start = DateTime.now().setZone(tz).minus({ days: 90 }).toISODate();
  UI.startDateInput.value = start;
  UI.endDateInput.value = end;

  // Auto-enable mock mode from URL parameter
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.has('mock') || urlParams.has('largedata')) {
    state.mockMode = true;
    UI.mockModeToggle.checked = true;
    if (urlParams.has('largedata')) {
      setBanner('info', `Large dataset mode: ${urlParams.get('largedata')} pages. Mock mode auto-enabled.`);
    }
  }

  // Optional: show a hint that mock has demo timezones if desired
  // (kept for future; MOCK_TIMEZONES imported to avoid dead-code linting in editors)

  workerRuntime.preferred = workerRuntime.supported;
  state.workerPreference = workerRuntime.preferred;
  if (UI.workerToggle) UI.workerToggle.checked = workerRuntime.preferred;
  updateWorkerStatus(workerRuntime.supported ? 'Initializing Web Worker…' : 'Web Worker unavailable; using main thread.');
  initWorker();

  // Initialize backpressure override drawer
  initBackpressureDrawer();

  clearBanner();
})();
