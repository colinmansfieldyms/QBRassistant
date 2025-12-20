import { getMockPage } from './mock-data.js';

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
export const CONCURRENCY_START = 4;
export const CONCURRENCY_MAX = 12;
export const RETRY_LIMIT = 2;
export const BACKOFF_BASE_MS = 400;
export const BACKOFF_JITTER = 180;
const SUCCESS_RAMP_THRESHOLD = 6;

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

async function fetchWithTimeout(url, options, { timeoutMs = 30000, outerSignal } = {}) {
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

async function fetchJson(url, { headers, outerSignal, timeoutMs = 30000 } = {}) {
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

function createRequestScheduler({ min = CONCURRENCY_MIN, start = CONCURRENCY_START, max = CONCURRENCY_MAX, signal, onAdaptiveChange } = {}) {
  let concurrency = Math.min(Math.max(start, min), max);
  let active = 0;
  const queue = [];
  let cancelled = false;
  let successStreak = 0;

  const cancel = (reason = 'Aborted') => {
    cancelled = true;
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

  const recordTransientError = () => {
    successStreak = 0;
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

  const scheduleNext = () => {
    if (cancelled) return;
    while (active < concurrency && queue.length) {
      const job = queue.shift();
      active++;
      job.fn()
        .then((res) => {
          successStreak += 1;
          maybeRampUp();
          job.resolve(res);
        })
        .catch((err) => {
          if (job.onTransient?.(err)) recordTransientError();
          job.reject(err);
        })
        .finally(() => {
          active--;
          scheduleNext();
        });
    }
  };

  const enqueue = (fn, { onTransient } = {}) => new Promise((resolve, reject) => {
    if (cancelled) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }
    queue.push({ fn, resolve, reject, onTransient });
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
      scheduler?.recordTransientError?.();
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
    const url = buildReportUrl({ tenant, report, facility, startDate, endDate, page });
    return fetchJson(url, { headers: headers(), outerSignal });
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
}) {
  const scheduler = createRequestScheduler({
    min: CONCURRENCY_MIN,
    start: CONCURRENCY_START,
    max: CONCURRENCY_MAX,
    signal,
    onAdaptiveChange,
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
      }), { onTransient: (err) => classifyError(err).transient });

      declaredLastPage = Math.max(1, Number(firstPayload?.last_page || 1));
      handlePage(firstPayload, 1);

      const remainingTasks = [];
      for (let p = 2; p <= declaredLastPage; p++) {
        remainingTasks.push(
          scheduler.enqueue(() => fetchReportPage({
            tenant,
            report,
            facility,
            startDate,
            endDate,
            page: p,
            tokenGetter,
            mockMode,
            outerSignal: signal,
            onWarning,
            scheduler,
          }), { onTransient: (err) => classifyError(err).transient })
            .then((payload) => handlePage(payload, p))
        );
      }

      await Promise.all(remainingTasks);
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
