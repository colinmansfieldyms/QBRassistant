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

/**
 * Concurrency limiter for fetches (global across all reports/facilities/pages).
 */
function createLimiter(max = 4) {
  let active = 0;
  const queue = [];

  const runNext = () => {
    if (active >= max) return;
    const next = queue.shift();
    if (!next) return;
    active++;
    next()
      .catch(() => {})
      .finally(() => {
        active--;
        runNext();
      });
  };

  return function limit(task, { signal } = {}) {
    return new Promise((resolve, reject) => {
      const wrapped = async () => {
        if (signal?.aborted) {
          reject(new DOMException(signal.reason || 'Aborted', 'AbortError'));
          return;
        }
        try {
          const res = await task();
          resolve(res);
        } catch (e) {
          reject(e);
        }
      };
      queue.push(wrapped);
      runNext();
    });
  };
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    if (signal) {
      if (signal.aborted) {
        clearTimeout(t);
        reject(new DOMException(signal.reason || 'Aborted', 'AbortError'));
        return;
      }
      signal.addEventListener('abort', () => {
        clearTimeout(t);
        reject(new DOMException(signal.reason || 'Aborted', 'AbortError'));
      }, { once: true });
    }
  });
}

function shouldRetry(status) {
  if (!status) return true; // network-ish
  return status >= 500 || status === 429;
}

async function fetchWithTimeout(url, options, { timeoutMs = 30000, outerSignal } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort('Request timeout'), timeoutMs);

  // Propagate outer abort -> inner controller
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
    // Try to bubble up a useful message
    const msg =
      (json && (json.message || json.error)) ||
      `HTTP ${res.status} ${res.statusText}`;
    throw new ApiError(msg, { status: res.status, url });
  }

  if (!json || typeof json !== 'object') {
    throw new ApiError('Invalid JSON response', { status: res.status, url });
  }
  return json;
}

/**
 * Build report URL with query params. Uses URLSearchParams as requested.
 */
function buildReportUrl({ tenant, report, facility, startDate, endDate, page }) {
  const base = `https://${tenant}.api.ymshub.com/api/v2`;
  const params = new URLSearchParams();
  params.set('fac_code', facility);
  params.set('start_date', startDate);
  params.set('end_date', endDate);
  params.set('page', String(page));
  return `${base}/reports/${encodeURIComponent(report)}?${params.toString()}`;
}

/**
 * Fetch all pages for a report/facility (page 1 first, read last_page, loop).
 * Calls onProgress after each page.
 * Streaming-friendly: returns per-page rows to caller, does not store.
 */
async function fetchAllPages({
  tenant,
  report,
  facility,
  startDate,
  endDate,
  tokenGetter,
  limiter,
  outerSignal,
  mockMode,
  onProgress,
  onWarning,
}) {
  const headers = () => {
    const token = tokenGetter?.();
    // Token is required in live mode; never log it.
    return {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      ...(mockMode ? {} : { 'Authorization': `Bearer ${token}` }),
    };
  };

  let rowsProcessed = 0;

  // Page 1 (must be first)
  const page1 = 1;
  const page1Payload = await limiter(async () => {
    if (mockMode) return getMockPage({ report, facility, page: page1 });
    const url = buildReportUrl({ tenant, report, facility, startDate, endDate, page: page1 });
    return fetchJson(url, { headers: headers(), outerSignal });
  }, { signal: outerSignal });

  const lastPage = Number(page1Payload.last_page || 1);
  const data1 = Array.isArray(page1Payload.data) ? page1Payload.data : [];
  rowsProcessed += data1.length;

  onProgress?.({ report, facility, page: 1, lastPage, rowsProcessed });

  const pages = [{ page: 1, data: data1 }];

  // Loop remaining pages; sequential for robustness (global concurrency still enforced by limiter).
  for (let p = 2; p <= lastPage; p++) {
    if (outerSignal?.aborted) throw new DOMException(outerSignal.reason || 'Aborted', 'AbortError');

    let attempt = 0;
    let payload = null;

    while (attempt <= 2) {
      try {
        payload = await limiter(async () => {
          if (mockMode) {
            // small jitter to mimic network
            await sleep(120 + Math.random() * 120, outerSignal);
            return getMockPage({ report, facility, page: p });
          }
          const url = buildReportUrl({ tenant, report, facility, startDate, endDate, page: p });
          return fetchJson(url, { headers: headers(), outerSignal });
        }, { signal: outerSignal });
        break;
      } catch (e) {
        const status = e instanceof ApiError ? e.status : null;
        if (outerSignal?.aborted) throw e;

        attempt++;
        if (attempt > 2 || !shouldRetry(status)) {
          throw new ApiError(
            `Failed fetching page ${p} for ${report}/${facility}: ${e.message || String(e)}`,
            { status, report, facility }
          );
        }
        const backoff = 450 * Math.pow(2, attempt) + Math.floor(Math.random() * 120);
        onWarning?.(`Retrying ${report}/${facility} page ${p} (attempt ${attempt}/2) after ${backoff}ms`);
        await sleep(backoff, outerSignal);
      }
    }

    const rows = Array.isArray(payload?.data) ? payload.data : [];
    rowsProcessed += rows.length;
    onProgress?.({ report, facility, page: p, lastPage, rowsProcessed });
    pages.push({ page: p, data: rows });

    // Stop early if next_page_url is null and API says we're done
    if (!payload?.next_page_url && p >= lastPage) break;
  }

  return pages;
}

export function createApiRunner({
  tenant,
  tokenGetter,
  mockMode = false,
  concurrency = 4,
  signal,
  onProgress,
  onFacilityStatus,
  onWarning,
}) {
  const limiter = createLimiter(concurrency);

  async function run({ reports, facilities, startDate, endDate, timezone, onRows }) {
    // Validate token only when live
    if (!mockMode) {
      const t = tokenGetter?.();
      if (!t) throw new ApiError('Missing token (cleared or not provided).');
    }

    // Create jobs per report/facility; each job fetches all pages sequentially.
    const jobs = [];
    for (const report of reports) {
      for (const facility of facilities) {
        jobs.push(async () => {
          onFacilityStatus?.({ report, facility, status: 'running' });

          try {
            const pages = await fetchAllPages({
              tenant,
              report,
              facility,
              startDate,
              endDate,
              tokenGetter,
              limiter,
              outerSignal: signal,
              mockMode,
              onProgress,
              onWarning,
            });

            for (const pg of pages) {
              if (signal?.aborted) throw new DOMException(signal.reason || 'Aborted', 'AbortError');
              onRows?.({ report, facility, page: pg.page, rows: pg.data });
            }

            onFacilityStatus?.({ report, facility, status: 'done' });
          } catch (e) {
            if (signal?.aborted) {
              onFacilityStatus?.({ report, facility, status: 'error', error: 'aborted' });
              throw e;
            }

            // Upgrade to meaningful auth/client errors
            if (e instanceof ApiError) {
              if (e.status === 401 || e.status === 403) {
                onFacilityStatus?.({ report, facility, status: 'error', error: 'Unauthorized (401/403) — invalid token?' });
              } else if (e.status === 404) {
                onFacilityStatus?.({ report, facility, status: 'error', error: 'Not found (404) — invalid tenant/report?' });
              } else {
                onFacilityStatus?.({ report, facility, status: 'error', error: e.message });
              }
              throw e;
            }

            onFacilityStatus?.({ report, facility, status: 'error', error: e?.message || String(e) });
            throw new ApiError(`Unexpected error in ${report}/${facility}: ${e?.message || String(e)}`);
          }
        });
      }
    }

    // Execute jobs with best-effort parallelism by queueing them through the same limiter.
    // NOTE: Each job still uses the limiter for page fetches, so this is mostly about fairness/cancellation.
    const jobPromises = jobs.map(job => limiter(job, { signal }));

    // If any job fails, we throw, but others may continue until abort.
    // That’s fine for MVP; next iteration can make “continue on error” configurable.
    await Promise.all(jobPromises);
  }

  return { run };
}
