import { getMockPage } from './mock-data.js?v=2025.01.07.0';

export class ApiError extends Error {
  constructor(message, { status = null, report = null, facility = null, url = null } = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.report = report;
    this.facility = facility;
    this.url = url;
  }
}

export const CONCURRENCY_MIN = 4;
export const CONCURRENCY_START = 8;
export const CONCURRENCY_MAX = 20;
export const PER_REPORT_LIMITS = {
  driver_history: { max: 6, min: 2 },
  default: { max: 18, min: 3 },
};
export const LATENCY_TARGETS = {
  // If p90 latency for a report lane spikes beyond this, back off that lane first.
  p90SpikeMs: 2600,
  // When p90 drops below this and the lane has headroom, gently recover.
  p90RecoverMs: 1700,
  sampleSize: 40,
};
export const RETRY_LIMIT = 2;
export const BACKOFF_BASE_MS = 400;
export const BACKOFF_JITTER = 180;
export const DEFAULT_TIMEOUT_MS = 60000;
export const SLOW_FIRST_PAGE_TIMEOUT_MS = 90000;
const SUCCESS_RAMP_THRESHOLD = 5;
const PAGE_QUEUE_LIMIT = 60; // Avoid enqueuing thousands of page fetches at once.
const SLOW_FIRST_PAGE_REPORTS = new Set(['driver_history']);

function abortableSleep(ms, signal) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    if (signal) {
      if (signal.aborted) {
        clearTimeout(t);
        reject(new DOMException(signal.reason || 'Aborted', 'AbortError'));
        return;
      }
      const onAbort = () => {
        clearTimeout(t);
        reject(new DOMException(signal.reason || 'Aborted', 'AbortError'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}

async function fetchWithTimeout(url, options, { timeoutMs = DEFAULT_TIMEOUT_MS, outerSignal } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort('Request timeout'), timeoutMs);

  const onAbort = () => {
    try { ctrl.abort(outerSignal.reason || 'Aborted'); } catch {}
  };
  if (outerSignal) {
    if (outerSignal.aborted) onAbort();
    else outerSignal.addEventListener('abort', onAbort, { once: true });
  }

  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(timer);
    if (outerSignal) outerSignal.removeEventListener?.('abort', onAbort);
  }
}

async function fetchJson(url, { headers, outerSignal, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const res = await fetchWithTimeout(url, { method: 'GET', headers }, { timeoutMs, outerSignal });

  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }

  if (!res.ok) {
    const msg = (json && (json.message || json.error)) || `HTTP ${res.status} ${res.statusText}`;
    throw new ApiError(msg, { status: res.status, url });
  }

  if (!json || typeof json !== 'object') {
    throw new ApiError('Invalid JSON response', { status: res.status, url });
  }
  return json;
}

function buildReportUrl({ tenant, report, facility, startDate, endDate, page }) {
  const base = `https://${tenant}.api.ymshub.com/api/v2`;
  const params = new URLSearchParams();
  params.set('fac_code', facility);
  params.set('start_date', startDate);
  params.set('end_date', endDate);
  params.set('page', String(page));
  return `${base}/reports/${encodeURIComponent(report)}?${params.toString()}`;
}

function classifyError(err) {
  const status = err instanceof ApiError ? err.status : (typeof err?.status === 'number' ? err.status : null);
  const transient = !status || status === 408 || status === 429 || status >= 500;
  const auth = status === 401 || status === 403;
  const client = status === 400 || status === 404;
  return { status, transient, auth, client };
}

function wrapApiError(err, context = {}) {
  if (err instanceof ApiError) return err;
  const { status } = classifyError(err);
  const suffix = context.report && context.facility
    ? ` for ${context.report}/${context.facility}${context.page ? ` page ${context.page}` : ''}`
    : '';
  const msg = `Request failed${suffix}: ${err?.message || String(err)}`;
  return new ApiError(msg, { status, report: context.report, facility: context.facility });
}

function createLatencyTracker({ maxSamples = 50 } = {}) {
  const perReport = new Map();

  const push = (report, ms) => {
    if (!perReport.has(report)) perReport.set(report, []);
    const arr = perReport.get(report);
    arr.push(ms);
    if (arr.length > maxSamples) arr.shift();
  };

  const p90 = (report) => {
    const arr = perReport.get(report) || [];
    if (!arr.length) return null;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.9));
    return sorted[idx];
  };

  const clear = () => perReport.clear();

  return { push, p90, clear };
}

function createRequestScheduler({
  min = CONCURRENCY_MIN,
  start = CONCURRENCY_START,
  max = CONCURRENCY_MAX,
  signal,
  onAdaptiveChange,
  onLaneChange,
  onRequestTiming,
  perReportLimits = PER_REPORT_LIMITS,
  latencyTargets = LATENCY_TARGETS,
} = {}) {
  let concurrency = Math.min(Math.max(start, min), max);
  let active = 0;
  const queue = [];
  let cancelled = false;
  let successStreak = 0;
  const laneUsage = new Map(); // report -> active count
  const laneCaps = new Map(); // report -> { max, min }
  const latency = createLatencyTracker({ maxSamples: latencyTargets.sampleSize });

  const cancel = (reason = 'Aborted') => {
    cancelled = true;
    latency.clear();
    while (queue.length) {
      const job = queue.shift();
      job.reject(new DOMException(reason, 'AbortError'));
    }
  };

  if (signal) {
    const onAbort = () => cancel(signal.reason || 'Aborted');
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
  }

  const recordTransientError = (report) => {
    successStreak = 0;
    const lane = laneCaps.get(report) || perReportLimits.default || { max, min };
    const currentLane = laneUsage.get(report) || 0;
    const laneNext = Math.max(lane.min || min, Math.max(1, Math.floor(currentLane / 2)));
    if (laneNext < (laneUsage.get(report) || lane.max || max)) {
      laneCaps.set(report, { ...lane, max: laneNext });
      onLaneChange?.({ report, direction: 'down', limit: laneNext });
    }

    const next = Math.max(min, Math.floor(concurrency / 2) || min);
    if (next < concurrency) {
      concurrency = next;
      onAdaptiveChange?.({ direction: 'down', concurrency });
    }
  };

  const maybeRampUp = () => {
    if (queue.length === 0) return;
    if (successStreak >= SUCCESS_RAMP_THRESHOLD && concurrency < max) {
      concurrency += 1;
      successStreak = 0;
      onAdaptiveChange?.({ direction: 'up', concurrency });
    }
  };

  const getLaneCap = (report) => {
    if (!laneCaps.has(report)) {
      const preset = perReportLimits[report] || perReportLimits.default || {};
      const cap = {
        max: Math.min(Math.max(preset.max ?? max, min), max),
        min: Math.max(preset.min ?? min, 1),
      };
      laneCaps.set(report, cap);
    }
    return laneCaps.get(report);
  };

  const scheduleNext = () => {
    if (cancelled) return;
    while (active < concurrency && queue.length) {
      const job = queue.shift();
      if (!job) return;
      const laneCap = getLaneCap(job.report);
      const laneActive = laneUsage.get(job.report) || 0;
      if (laneActive >= (laneCap.max || concurrency)) {
        queue.push(job); // defer; lane saturated
        if (queue.length === 1) return; // avoid tight loop if only this job exists
        continue;
      }

      active++;
      laneUsage.set(job.report, laneActive + 1);

      const startedAt = performance.now();

      job.fn()
        .then((res) => {
          const elapsed = performance.now() - startedAt;
          latency.push(job.report, elapsed);
          onRequestTiming?.({ report: job.report, ms: elapsed });

          const p90 = latency.p90(job.report);
          const targets = latencyTargets || {};
          if (p90 && targets.p90SpikeMs && p90 > targets.p90SpikeMs) {
            const currentCap = getLaneCap(job.report);
            const down = Math.max(currentCap.min || min, Math.max(1, Math.floor((currentCap.max || concurrency) / 2)));
            if (down < (currentCap.max || concurrency)) {
              laneCaps.set(job.report, { ...currentCap, max: down });
              onLaneChange?.({ report: job.report, direction: 'down', limit: down, reason: 'latency' });
            }
          } else if (p90 && targets.p90RecoverMs && p90 < targets.p90RecoverMs) {
            const currentCap = getLaneCap(job.report);
            const upCandidate = Math.min((currentCap.max || concurrency) + 1, (perReportLimits[job.report]?.max ?? perReportLimits.default?.max ?? max));
            if (upCandidate > (currentCap.max || concurrency)) {
              laneCaps.set(job.report, { ...currentCap, max: upCandidate });
              onLaneChange?.({ report: job.report, direction: 'up', limit: upCandidate, reason: 'latency_recover' });
            }
          }

          successStreak += 1;
          maybeRampUp();
          job.resolve(res);
        })
        .catch((err) => {
          if (job.onTransient?.(err)) recordTransientError(job.report);
          job.reject(err);
        })
        .finally(() => {
          active--;
          const laneActiveNow = (laneUsage.get(job.report) || 1) - 1;
          if (laneActiveNow <= 0) laneUsage.delete(job.report);
          else laneUsage.set(job.report, laneActiveNow);
          scheduleNext();
        });
    }
  };

  const enqueue = (fn, { onTransient, report } = {}) => new Promise((resolve, reject) => {
    if (cancelled) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }
    queue.push({ fn, resolve, reject, onTransient, report });
    scheduleNext();
  });

  return { enqueue, cancel, recordTransientError, getConcurrency: () => concurrency };
}

async function executeWithRetry(task, { signal, onWarning, context, scheduler } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await task();
    } catch (err) {
      if (signal?.aborted) throw err;
      const { transient, auth, client, status } = classifyError(err);
      if (auth || client || !transient || attempt >= RETRY_LIMIT) {
        throw wrapApiError(err, context);
      }
      scheduler?.recordTransientError?.(context?.report);
      const backoff = BACKOFF_BASE_MS * Math.pow(2, attempt) + Math.floor(Math.random() * BACKOFF_JITTER);
      onWarning?.(`Retrying ${context.report}/${context.facility} page ${context.page} after ${backoff}ms (attempt ${attempt + 1}/${RETRY_LIMIT}).`);
      await abortableSleep(backoff, signal);
      attempt += 1;
    }
  }
}

async function fetchReportPage({
  tenant,
  report,
  facility,
  startDate,
  endDate,
  page,
  tokenGetter,
  mockMode,
  outerSignal,
  onWarning,
  scheduler,
}) {
  const headers = () => {
    const token = tokenGetter?.();
    if (!mockMode && !token) {
      throw new ApiError('Missing token (cleared or not provided).', { status: 401 });
    }
    return {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      ...(mockMode ? {} : { 'Authorization': `Bearer ${token}` }),
    };
  };

  return executeWithRetry(async () => {
    if (mockMode) {
      await abortableSleep(80 + Math.random() * 80, outerSignal);
      return getMockPage({ report, facility, page });
    }
    const timeoutMs = (page === 1 && SLOW_FIRST_PAGE_REPORTS.has(report))
      ? SLOW_FIRST_PAGE_TIMEOUT_MS
      : DEFAULT_TIMEOUT_MS;
    const url = buildReportUrl({ tenant, report, facility, startDate, endDate, page });
    return fetchJson(url, { headers: headers(), outerSignal, timeoutMs });
  }, {
    signal: outerSignal,
    onWarning,
    scheduler,
    context: { report, facility, page },
  });
}

export function createApiRunner({
  tenant,
  tokenGetter,
  mockMode = false,
  signal,
  onProgress,
  onFacilityStatus,
  onWarning,
  onAdaptiveChange,
  onLaneChange,
  onPerf,
}) {
  const scheduler = createRequestScheduler({
    min: CONCURRENCY_MIN,
    start: CONCURRENCY_START,
    max: CONCURRENCY_MAX,
    signal,
    onAdaptiveChange,
    onLaneChange,
    onRequestTiming: (payload) => {
      if (!payload || !onPerf) return;
      onPerf({ type: 'request', ...payload });
    },
  });

  async function processReportFacility({ report, facility, startDate, endDate, timezone, onRows }) {
    onFacilityStatus?.({ report, facility, status: 'running' });

    let rowsProcessed = 0;
    let declaredLastPage = 1;
    let stopAtPage = null;

    const handlePage = (payload, pageNumber) => {
      if (!payload) return;
      if (stopAtPage && pageNumber > stopAtPage) return;
      const rows = Array.isArray(payload.data) ? payload.data : [];
      if (typeof payload.last_page === 'number') {
        declaredLastPage = Math.max(declaredLastPage, payload.last_page || 1);
      }
      if (!payload.next_page_url) {
        stopAtPage = stopAtPage ? Math.min(stopAtPage, pageNumber) : pageNumber;
      }
      const effectiveLastPage = stopAtPage ? stopAtPage : declaredLastPage;
      rowsProcessed += rows.length;
      onProgress?.({ report, facility, page: pageNumber, lastPage: effectiveLastPage, rowsProcessed });
      onRows?.({ report, facility, page: pageNumber, lastPage: effectiveLastPage, rows });
    };

    try {
      const firstPayload = await scheduler.enqueue(() => fetchReportPage({
        tenant,
        report,
        facility,
        startDate,
        endDate,
        page: 1,
        tokenGetter,
        mockMode,
        outerSignal: signal,
        onWarning,
        scheduler,
      }), { onTransient: (err) => classifyError(err).transient, report });

      declaredLastPage = Math.max(1, Number(firstPayload?.last_page || 1));
      handlePage(firstPayload, 1);

      let nextPage = 2;
      let effectiveLastPage = declaredLastPage;
      const inflight = new Set();

      const getBufferLimit = () => {
        const dynamic = Math.max(CONCURRENCY_MIN, (scheduler.getConcurrency?.() || CONCURRENCY_START) * 2);
        return Math.min(PAGE_QUEUE_LIMIT, dynamic);
      };

      const pump = () => {
        if (signal?.aborted) return;
        const stopPage = stopAtPage || effectiveLastPage;
        const bufferLimit = getBufferLimit();

        while (inflight.size < bufferLimit && nextPage <= stopPage) {
          const pageNumber = nextPage++;
          const task = scheduler.enqueue(() => fetchReportPage({
            tenant,
            report,
            facility,
            startDate,
            endDate,
            page: pageNumber,
            tokenGetter,
            mockMode,
            outerSignal: signal,
            onWarning,
            scheduler,
          }), { onTransient: (err) => classifyError(err).transient, report })
            .then((payload) => {
              if (payload && typeof payload.last_page === 'number') {
                effectiveLastPage = Math.max(effectiveLastPage, payload.last_page || pageNumber);
              }
              handlePage(payload, pageNumber);
            })
            .finally(() => {
              inflight.delete(task);
              // Immediately pump more pages as tasks complete
              pump();
            });

          inflight.add(task);
        }
      };

      pump();

      while (inflight.size > 0) {
        await Promise.race(inflight);
        // No need to pump here since it's called in finally()
      }

      onFacilityStatus?.({ report, facility, status: 'done' });
    } catch (e) {
      if (signal?.aborted) {
        onFacilityStatus?.({ report, facility, status: 'error', error: 'aborted' });
        throw e;
      }

      const status = e instanceof ApiError ? e.status : null;
      if (e instanceof ApiError && (status === 401 || status === 403)) {
        scheduler.cancel('auth failure');
        onFacilityStatus?.({ report, facility, status: 'error', error: 'Unauthorized (401/403) — invalid token?' });
      } else if (e instanceof ApiError && status === 404) {
        onFacilityStatus?.({ report, facility, status: 'error', error: 'Not found (404) — invalid tenant/report?' });
      } else {
        onFacilityStatus?.({ report, facility, status: 'error', error: e?.message || String(e) });
      }
      throw e;
    }
  }

  async function run({ reports, facilities, startDate, endDate, timezone, onRows }) {
    if (!mockMode) {
      const t = tokenGetter?.();
      if (!t) throw new ApiError('Missing token (cleared or not provided).');
    }

    const tasks = [];
    for (const report of reports) {
      for (const facility of facilities) {
        tasks.push(processReportFacility({ report, facility, startDate, endDate, timezone, onRows }));
      }
    }

    try {
      await Promise.all(tasks);
    } catch (err) {
      scheduler.cancel('failure');
      throw err;
    }
  }

  return { run };
}
