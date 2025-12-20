import { createApiRunner, ApiError } from './api.js';
import { createAnalyzers, normalizeRowStrict } from './analysis.js';
import { renderReportResult, destroyAllCharts } from './charts.js';
import { downloadText, downloadCsv, buildSummaryTxt, buildReportSummaryCsv, buildChartCsv, printReport } from './export.js';
import { MOCK_TIMEZONES } from './mock-data.js';

const { DateTime } = window.luxon;

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
  if (state.abortController) {
    try { state.abortController.abort(reason); } catch {}
  }
  cancelWorkerRun(reason);
}

function resetAll() {
  abortInFlight('Reset all.');
  destroyAllCharts(state.chartRegistry);
  state.chartRegistry.clear();

  state.running = false;
  state.abortController = null;
  state.runStartedAt = null;
  state.inputs = null;
  state.warnings = [];
  state.progress = {};
  state.results = {};
  resetWorkerState();

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

function updateProgressUI(report) {
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

function addWarning(msg) {
  const stamp = DateTime.now().setZone(state.timezone).toFormat('yyyy-LL-dd HH:mm:ss');
  state.warnings.push(`[${stamp}] ${msg}`);
  UI.warningsPanel.textContent = state.warnings.length ? state.warnings.join('\n') : 'None.';
}

// ---------- Worker helpers ----------
function updateWorkerStatus(text) {
  if (UI.workerStatus) UI.workerStatus.textContent = text;
  if (UI.workerToggle) {
    UI.workerToggle.disabled = !workerRuntime.supported || !!workerRuntime.fallbackReason || state.running;
    UI.workerToggle.checked = workerRuntime.supported && !workerRuntime.fallbackReason && workerRuntime.preferred;
  }
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
        updateProgressUI(data.report);
      }
      break;
    case 'PARTIAL_RESULT':
      if (data.runId !== workerRuntime.currentRunId) return;
      if (data.results && state.running) {
        state.results = data.results;
        renderAllResults();
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

  initProgressUI(inputs.reports, inputs.facilities);
  setRunningUI(true);
  setBanner('info', 'Running assessment… streaming pages, updating metrics, discarding raw rows.');

  state.abortController = new AbortController();
  const signal = state.abortController.signal;

  const roiEnabled = canComputeROI(inputs.assumptions);
  const useWorker = shouldUseWorker();
  const workerRun = useWorker ? beginWorkerRun({
    timezone: inputs.timezone,
    startDate: inputs.startDate,
    endDate: inputs.endDate,
    assumptions: inputs.assumptions,
    selectedReports: inputs.reports,
    facilities: inputs.facilities,
    tenant: inputs.tenant,
    roiEnabled,
  }) : null;

  if (useWorker && !workerRun) {
    addWarning('Web Worker preferred but unavailable; using main-thread analysis.');
  }

  const analyzers = workerRun ? null : createAnalyzers({
    timezone: inputs.timezone,
    startDate: inputs.startDate,
    endDate: inputs.endDate,
    assumptions: inputs.assumptions,
    onWarning: (w) => addWarning(w),
  });

  const apiRunner = createApiRunner({
    tenant: inputs.tenant,
    tokenGetter: () => state.token, // token remains in-memory only
    mockMode: state.mockMode,
    concurrency: 4,
    signal,
    onProgress: ({ report, facility, page, lastPage, rowsProcessed }) => {
      const p = state.progress?.[report]?.[facility];
      if (!p) return;
      p.status = 'running';
      p.page = page;
      p.lastPage = lastPage;
      p.rowsProcessed = rowsProcessed;
      updateProgressUI(report);
    },
    onFacilityStatus: ({ report, facility, status, error }) => {
      const p = state.progress?.[report]?.[facility];
      if (!p) return;
      p.status = status;
      p.error = error || null;
      updateProgressUI(report);
    },
    onWarning: addWarning,
  });

  // Run each report/facility streaming -> analyzer
  try {
    await apiRunner.run({
      reports: inputs.reports,
      facilities: inputs.facilities,
      startDate: inputs.startDate,
      endDate: inputs.endDate,
      timezone: inputs.timezone,
      onRows: ({ report, facility, page, lastPage, rows }) => {
        if (workerRun) {
          workerRuntime.worker?.postMessage({
            type: 'PAGE_ROWS',
            runId: workerRun.runId,
            report,
            facility,
            page,
            lastPage,
            rows,
          });
          return;
        }

        const analyzer = analyzers?.[report];
        if (!analyzer) return;

        for (const raw of rows) {
          const normalized = normalizeRowStrict(raw, { report, timezone: inputs.timezone, onWarning: addWarning });
          if (normalized) analyzer.ingest(normalized);
        }
      }
    });

    // Finalize per report
    if (workerRun) {
      const payload = await finalizeWorkerRun(workerRun.runId);
      if (Array.isArray(payload?.warnings)) payload.warnings.forEach(addWarning);
      state.results = payload?.results || {};
    } else {
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

    renderAllResults();
    setBanner('ok', 'Complete. Token wiped from memory. You can export summaries, print, and download chart CSV/PNG.');

  } catch (e) {
    if (workerRun) cancelWorkerRun('Run failed');
    if (signal.aborted) {
      setBanner('info', 'Run cancelled. Token wiped from memory.');
    } else if (e instanceof ApiError) {
      setBanner('error', `API error: ${e.message}`);
      addWarning(`API error: ${e.message}`);
    } else {
      setBanner('error', `Unexpected error: ${e?.message || String(e)}`);
      addWarning(`Unexpected error: ${e?.stack || e?.message || String(e)}`);
    }
  } finally {
    // Critical: null out token variables and abort controller refs
    setTokenInMemory(null);
    state.abortController = null;
    setRunningUI(false);

    // Also clear token field to avoid “token lingering in DOM”
    UI.tokenInput.value = '';
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
  // token wiped from memory immediately, and cancel aborts in-flight requests
  setTokenInMemory(null);
  UI.tokenInput.value = '';
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

// ---------- Init ----------
(function init() {
  buildTimezoneOptions(UI.timezoneSelect);

  // default dates: last 90 days
  const tz = state.timezone;
  const end = DateTime.now().setZone(tz).toISODate();
  const start = DateTime.now().setZone(tz).minus({ days: 90 }).toISODate();
  UI.startDateInput.value = start;
  UI.endDateInput.value = end;

  // Optional: show a hint that mock has demo timezones if desired
  // (kept for future; MOCK_TIMEZONES imported to avoid dead-code linting in editors)

  workerRuntime.preferred = workerRuntime.supported;
  state.workerPreference = workerRuntime.preferred;
  if (UI.workerToggle) UI.workerToggle.checked = workerRuntime.preferred;
  updateWorkerStatus(workerRuntime.supported ? 'Initializing Web Worker…' : 'Web Worker unavailable; using main thread.');
  initWorker();

  clearBanner();
})();
