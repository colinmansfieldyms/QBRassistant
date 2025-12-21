import { getMockPage } from './mock-data.js?v=2025.01.07.0';
import { instrumentation } from './instrumentation.js?v=2025.01.07.0';
import { getConfig, getEffectiveTier } from './backpressure-config.js';

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

export const CONCURRENCY_MIN = 2;
export const CONCURRENCY_START = 4;
export const CONCURRENCY_MAX = 8;
export const PER_REPORT_LIMITS = {
  driver_history: { max: 3, min: 1 },
  default: { max: 6, min: 2 },
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
export const GREEN_ZONE_ENABLED = true;
export const GREEN_ZONE_CONCURRENCY_MAX = 12;
export const GREEN_ZONE_LANE_BONUS = 2;
export const GREEN_ZONE_LANE_CAP = 10;
export const GREEN_ZONE_STREAK_COUNT = 4;
export const GREEN_ZONE_COOLDOWN_MS = 8000;
export const GREEN_ZONE_MEMORY_ENTER_RATIO = 0.58;
export const GREEN_ZONE_MEMORY_EXIT_RATIO = 0.78;
// Tune green zone behavior: *_MAX lifts ceilings, *_STREAK_COUNT gates entry, *_COOLDOWN_MS slows re-entry,
// *_RATIO thresholds gate memory headroom checks. Disable via GREEN_ZONE_ENABLED when needed.
const SUCCESS_RAMP_THRESHOLD = 5;
const MAX_IN_FLIGHT_PAGES = 6;     // Base network prefetch ceiling before adaptive controls
const YIELD_EVERY_N_PAGES = 1;     // Yield after every page for responsiveness
const SLOW_FIRST_PAGE_REPORTS = new Set(['driver_history']);
export const FETCH_BUFFER_MAX_DEFAULT = 9;
export const FETCH_BUFFER_MAX_MIN = 2;
export const PROCESSING_POOL_MAX_DEFAULT = 3;
export const PROCESSING_POOL_MAX_MIN = 1;
const PROCESSING_POOL_MAX_HARD = 4;
const LATENCY_ADJUST_COOLDOWN_MS = 5000;
const MEMORY_PRESSURE_THRESHOLD = 0.82;
const MEMORY_PRESSURE_RECOVER = 0.65;
const MEMORY_CHECK_INTERVAL_MS = 2500;
const YIELD_EVERY_PAGE_THRESHOLD = 1000;

// Dynamic backpressure thresholds based on total page count
// More conservative for extreme datasets to prevent browser freeze/crash
const BACKPRESSURE_TIERS = [
  { maxPages: 50, maxInFlight: 8, yieldEvery: 2, fetchBuffer: 10, processingMax: 4 },       // Small: moderate
  { maxPages: 200, maxInFlight: 6, yieldEvery: 1, fetchBuffer: 8, processingMax: 4 },       // Medium: conservative
  { maxPages: 500, maxInFlight: 4, yieldEvery: 1, fetchBuffer: 6, processingMax: 3 },       // Large: very conservative
  { maxPages: 1000, maxInFlight: 3, yieldEvery: 1, fetchBuffer: 5, processingMax: 3 },      // Very large: extremely conservative
  { maxPages: Infinity, maxInFlight: 2, yieldEvery: 1, fetchBuffer: 4, processingMax: 2 }   // Extreme (1k+): maximum conservation
];
const APPROX_STRING_CAP = 256;
const APPROX_COMPLEX_FIELD_BYTES = 16;

function getBackpressureConfig(totalPages) {
  const tier = BACKPRESSURE_TIERS.find(t => totalPages <= t.maxPages) || BACKPRESSURE_TIERS[BACKPRESSURE_TIERS.length - 1];
  return tier;
}

// Yield helper - gives control back to event loop
async function yieldToEventLoop() {
  instrumentation.recordYield();
  return new Promise(resolve => {
    if (typeof requestAnimationFrame !== 'undefined') {
      requestAnimationFrame(() => setTimeout(resolve, 0));
    } else {
      setTimeout(resolve, 0);
    }
  });
}

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

// Approximate payload sizing that avoids large JSON.stringify allocations.
// Counts key lengths and a capped slice of string values; numbers/booleans receive a tiny fixed weight.
export function estimatePayloadWeight(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { approxBytes: 0, fieldCount: 0, rowCount: Array.isArray(rows) ? rows.length : 0 };
  }

  let approxBytes = 0;
  let fieldCount = 0;
  const baseRowOverhead = 12; // Lightweight per-row overhead to mimic delimiters/metadata without stringifying

  for (const row of rows) {
    approxBytes += baseRowOverhead;
    if (!row || typeof row !== 'object') continue;
    for (const key in row) {
      if (!Object.prototype.hasOwnProperty.call(row, key)) continue;
      fieldCount += 1;
      approxBytes += key.length;
      const value = row[key];
      if (value === null || value === undefined) continue;
      if (typeof value === 'string') {
        approxBytes += Math.min(value.length, APPROX_STRING_CAP);
      } else if (typeof value === 'number') {
        approxBytes += 8;
      } else if (typeof value === 'boolean') {
        approxBytes += 4;
      } else if (Array.isArray(value)) {
        approxBytes += Math.min(value.length, APPROX_STRING_CAP / 2);
      } else {
        approxBytes += APPROX_COMPLEX_FIELD_BYTES;
      }
    }
  }

  return { approxBytes, fieldCount, rowCount: rows.length };
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

// Adaptive green zone controller:
// - Enter after sustained per-report latency streaks under p90RecoverMs while memory has headroom.
// - Exit quickly on latency spikes, memory pressure, or strict backpressure ceilings; cooldown prevents flapping.
export function createGreenZoneController({
  enabled = GREEN_ZONE_ENABLED,
  baseMax = CONCURRENCY_MAX,
  greenMax = GREEN_ZONE_CONCURRENCY_MAX,
  laneBonus = GREEN_ZONE_LANE_BONUS,
  laneCap = GREEN_ZONE_LANE_CAP,
  latencyStreak = GREEN_ZONE_STREAK_COUNT,
  cooldownMs = GREEN_ZONE_COOLDOWN_MS,
  latencyTargets = LATENCY_TARGETS,
  memoryEnterRatio = GREEN_ZONE_MEMORY_ENTER_RATIO,
  memoryExitRatio = GREEN_ZONE_MEMORY_EXIT_RATIO,
  nowFn = () => (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now()),
  memoryProvider = () => (typeof performance !== 'undefined' && performance.memory ? performance.memory : null),
} = {}) {
  const streaks = new Map();
  const observedReports = new Set();
  const entryThreshold = latencyTargets?.p90RecoverMs ?? 0;
  const exitThreshold = latencyTargets?.p90SpikeMs ?? Math.max(entryThreshold + 500, entryThreshold * 1.2);
  const state = {
    green: false,
    lastExit: 0,
    backpressureCeiling: null,
  };

  const inCooldown = () => {
    if (!state.lastExit) return false;
    return nowFn() - state.lastExit < cooldownMs;
  };

  const getMemoryRatio = () => {
    const mem = memoryProvider?.();
    if (!mem || !mem.jsHeapSizeLimit || !mem.usedJSHeapSize) return null;
    if (mem.jsHeapSizeLimit === 0) return null;
    return mem.usedJSHeapSize / mem.jsHeapSizeLimit;
  };

  const hasMemoryHeadroom = (forEntry) => {
    const ratio = getMemoryRatio();
    if (ratio === null) return true;
    const limit = forEntry ? memoryEnterRatio : memoryExitRatio;
    return ratio < limit;
  };

  const canEnter = () => {
    if (!enabled || state.green) return false;
    if (inCooldown()) return false;
    if (!hasMemoryHeadroom(true)) return false;
    if (state.backpressureCeiling && state.backpressureCeiling < baseMax) return false;
    if (!observedReports.size) return false;
    return [...observedReports].every((r) => (streaks.get(r) || 0) >= latencyStreak);
  };

  const recordSample = (report, p90) => {
    if (!enabled) return { green: false, changed: state.green };
    if (typeof report === 'string') observedReports.add(report);
    if (typeof p90 === 'number' && report) {
      if (p90 < entryThreshold) {
        streaks.set(report, (streaks.get(report) || 0) + 1);
      } else {
        streaks.set(report, 0);
      }
    }

    let changed = false;
    let reason = null;
    if (state.green) {
      const exitByLatency = typeof p90 === 'number' && p90 >= exitThreshold;
      const exitByMemory = !hasMemoryHeadroom(false);
      const exitByBackpressure = state.backpressureCeiling && state.backpressureCeiling < baseMax;
      if (exitByLatency || exitByMemory || exitByBackpressure) {
        state.green = false;
        state.lastExit = nowFn();
        changed = true;
        reason = exitByLatency ? 'latency_spike' : (exitByMemory ? 'memory_pressure' : 'backpressure');
      }
    } else if (canEnter()) {
      state.green = true;
      changed = true;
      reason = 'enter';
    }

    return { green: state.green, changed, reason, memoryRatio: getMemoryRatio() };
  };

  const setBackpressureCeiling = (limit) => {
    state.backpressureCeiling = typeof limit === 'number' && limit > 0 ? limit : null;
    if (state.green && state.backpressureCeiling && state.backpressureCeiling < baseMax) {
      state.green = false;
      state.lastExit = nowFn();
      return { changed: true, reason: 'backpressure' };
    }
    return { changed: false };
  };

  const getConcurrencyCeiling = () => {
    const backpressureCap = state.backpressureCeiling;
    if (!enabled) {
      return backpressureCap ? Math.min(baseMax, backpressureCap) : baseMax;
    }
    const desired = state.green
      ? Math.max(baseMax, Math.min(greenMax, backpressureCap ?? greenMax))
      : baseMax;
    const capped = backpressureCap ? Math.min(desired, backpressureCap) : desired;
    return Math.max(CONCURRENCY_MIN, capped);
  };

  const getLaneCeiling = (baseLaneMax, globalCeiling) => {
    const normalizedBase = Math.max(1, baseLaneMax || baseMax);
    const ceiling = state.green
      ? Math.min(globalCeiling, Math.min(normalizedBase + laneBonus, laneCap ?? greenMax, greenMax))
      : Math.min(normalizedBase, globalCeiling);
    return Math.max(1, ceiling);
  };

  return {
    recordSample,
    setBackpressureCeiling,
    getConcurrencyCeiling,
    getLaneCeiling,
    isGreen: () => state.green,
    lastExitAt: () => state.lastExit,
  };
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
  greenZoneConfig,
} = {}) {
  const greenZone = createGreenZoneController({
    baseMax: max,
    latencyTargets,
    ...greenZoneConfig,
  });
  let backpressureCeiling = null;
  const getEffectiveMax = () => {
    const zoneMax = greenZone.getConcurrencyCeiling();
    const cap = backpressureCeiling ? Math.min(zoneMax, backpressureCeiling) : zoneMax;
    return Math.max(min, cap);
  };
  let concurrency = Math.min(Math.max(start, min), getEffectiveMax());
  let active = 0;
  const queue = [];
  let cancelled = false;
  let successStreak = 0;
  const laneUsage = new Map(); // report -> active count
  const laneCaps = new Map(); // report -> { max, min }
  const latency = createLatencyTracker({ maxSamples: latencyTargets.sampleSize });
  const laneDefaults = new Map(); // report -> presets for reuse

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

    clampConcurrency('transient_error');
  };

  const clampConcurrency = (reason) => {
    const cap = getEffectiveMax();
    const next = Math.max(min, Math.min(cap, concurrency));
    if (next < concurrency) {
      concurrency = next;
      onAdaptiveChange?.({ direction: 'down', concurrency, reason });
    }
  };

  const setBackpressureCeiling = (limit) => {
    backpressureCeiling = typeof limit === 'number' && limit > 0 ? limit : null;
    const transition = greenZone.setBackpressureCeiling(backpressureCeiling);
    clampConcurrency(transition.changed ? 'backpressure' : 'backpressure_ceiling');
    refreshLaneCaps(transition.changed ? 'green_exit' : 'backpressure');
  };

  const maybeRampUp = () => {
    if (queue.length === 0) return;
    const cap = getEffectiveMax();
    if (successStreak >= SUCCESS_RAMP_THRESHOLD && concurrency < cap) {
      concurrency += 1;
      successStreak = 0;
      onAdaptiveChange?.({ direction: 'up', concurrency });
    }
  };

  const getLaneCap = (report) => {
    if (!laneDefaults.has(report)) {
      laneDefaults.set(report, perReportLimits[report] || perReportLimits.default || {});
    }
    const defaults = laneDefaults.get(report);
    if (!laneCaps.has(report)) {
      const cap = {
        min: Math.max(defaults?.min ?? min, 1),
        ceiling: greenZone.getLaneCeiling(defaults?.max ?? max, getEffectiveMax()),
      };
      cap.max = Math.min(Math.max(defaults?.max ?? max, cap.min), cap.ceiling);
      laneCaps.set(report, cap);
    }
    return laneCaps.get(report);
  };

  const refreshLaneCaps = (reason) => {
    laneCaps.forEach((cap, report) => {
      const defaults = laneDefaults.get(report) || perReportLimits[report] || perReportLimits.default || {};
      const ceiling = greenZone.getLaneCeiling(defaults?.max ?? max, getEffectiveMax());
      let nextMax = cap.max;
      if (cap.max > ceiling) {
        nextMax = ceiling;
      } else if (reason === 'green_enter' && ceiling > cap.ceiling) {
        nextMax = Math.min(ceiling, cap.max + 1);
      }
      laneCaps.set(report, { ...cap, max: nextMax, ceiling });
      if (nextMax !== cap.max) {
        onLaneChange?.({ report, direction: nextMax > cap.max ? 'up' : 'down', limit: nextMax, reason });
      }
    });
  };

  const scheduleNext = () => {
    if (cancelled) return;
    clampConcurrency();
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
            const upCandidate = Math.min((currentCap.max || concurrency) + 1, currentCap.ceiling || getEffectiveMax());
            if (upCandidate > (currentCap.max || concurrency)) {
              laneCaps.set(job.report, { ...currentCap, max: upCandidate });
              onLaneChange?.({ report: job.report, direction: 'up', limit: upCandidate, reason: 'latency_recover' });
            }
          }

          const zoneChange = greenZone.recordSample(job.report, p90);
          if (zoneChange.changed) {
            clampConcurrency(zoneChange.reason || (zoneChange.green ? 'green_enter' : 'green_exit'));
            refreshLaneCaps(zoneChange.green ? 'green_enter' : 'green_exit');
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

  return { enqueue, cancel, recordTransientError, getConcurrency: () => concurrency, setBackpressureCeiling };
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
  pipelineConfig,
}) {
  const runId = `run_${Date.now()}_${Math.random().toString(16).slice(2)}`;

  // Read backpressure overrides at run start (snapshot for consistency)
  const bpConfig = getConfig();

  const scheduler = createRequestScheduler({
    min: CONCURRENCY_MIN,
    start: CONCURRENCY_START,
    max: bpConfig.globalMaxConcurrency,
    signal,
    onAdaptiveChange,
    onLaneChange,
    onRequestTiming: (payload) => {
      if (!payload || !onPerf) return;
      onPerf({ type: 'request', ...payload });
    },
    greenZoneConfig: {
      enabled: bpConfig.greenZoneEnabled,
      greenMax: bpConfig.greenZoneConcurrencyMax,
      latencyStreak: bpConfig.greenZoneStreakCount,
    },
  });

  async function processReportFacility({
    report,
    facility,
    startDate,
    endDate,
    timezone,
    onRows,
    runIdCheck,
  }) {
    onFacilityStatus?.({ report, facility, status: 'running' });

    const facilityStats = {
      maxFetchBuffer: 0,
      maxBufferedPlusInFlight: 0,
      maxProcessingActive: 0,
      yieldCount: 0,
      progressEvents: 0,
      finalBuffer: 0,
      finalProcessing: 0,
    };

    const pipelineOptions = pipelineConfig || {};
    const yieldHelper = pipelineOptions.yieldFn || yieldToEventLoop;
    const performYield = async () => {
      facilityStats.yieldCount++;
      await yieldHelper();
      if (yieldHelper !== yieldToEventLoop) {
        instrumentation.recordYield();
      }
    };

    let rowsProcessed = 0;
    let declaredLastPage = 1;
    let stopAtPage = null;
    let pagesProcessed = 0;
    let backpressureConfig = null; // Will be set after first page
    let effectiveLastPage = 1;
    let pipelineError = null;
    const latencyTracker = createLatencyTracker({ maxSamples: LATENCY_TARGETS.sampleSize });

    const markError = (err) => {
      if (!pipelineError) pipelineError = err;
    };

    // Helper to get tier config, respecting forceTier override
    const getTierConfig = (totalPages) => {
      const tierResult = getEffectiveTier(totalPages);
      return tierResult.config || getBackpressureConfig(totalPages);
    };

    const deriveInitialFetchCap = (totalPages) => {
      const tier = backpressureConfig || getTierConfig(totalPages);
      const tierCap = tier?.fetchBuffer ?? tier?.maxInFlight ?? MAX_IN_FLIGHT_PAGES;
      // Use override fetch buffer size, falling back to tier/default
      const configuredBuffer = bpConfig.fetchBufferSize;
      const base = Math.min(
        Math.max(configuredBuffer, FETCH_BUFFER_MAX_MIN),
        bpConfig.pageQueueLimit
      );
      return Math.max(
        FETCH_BUFFER_MAX_MIN,
        Math.min(base, tierCap || base, bpConfig.pageQueueLimit)
      );
    };

    const deriveInitialProcessingCap = (totalPages) => {
      const tier = backpressureConfig || getTierConfig(totalPages);
      const tierCap = tier?.processingMax ?? PROCESSING_POOL_MAX_DEFAULT;
      // Use override processing pool size, falling back to tier/default
      const configuredPool = bpConfig.processingPoolSize;
      return Math.min(
        PROCESSING_POOL_MAX_HARD,
        Math.max(PROCESSING_POOL_MAX_MIN, Math.min(configuredPool, tierCap || configuredPool))
      );
    };

    const emitPipelinePerf = (extra = {}) => {
      onPerf?.({
        type: 'pipeline',
        report,
        facility,
        ...extra,
      });
    };

    const updateStats = (state) => {
      facilityStats.maxFetchBuffer = Math.max(facilityStats.maxFetchBuffer, state.fetchBufferLength);
      facilityStats.maxBufferedPlusInFlight = Math.max(
        facilityStats.maxBufferedPlusInFlight,
        state.fetchBufferLength + state.fetchInFlight
      );
      facilityStats.maxProcessingActive = Math.max(
        facilityStats.maxProcessingActive,
        state.processingActive
      );
    };

    const handlePage = async (payload, pageNumber) => {
      // Check cancellation via runId
      if (runIdCheck && runIdCheck() !== runId) return;
      if (signal?.aborted) return;
      if (!payload) return;
      if (stopAtPage && pageNumber > stopAtPage) return;

      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      const effectivePageCap = stopAtPage ? stopAtPage : declaredLastPage;
      rowsProcessed += rows.length;

      // Record metrics
      const payloadSize = payload.payloadBytes ?? estimatePayloadWeight(rows).approxBytes;
      instrumentation.recordPageComplete(payloadSize);

      onProgress?.({ report, facility, page: pageNumber, lastPage: effectivePageCap, rowsProcessed });
      facilityStats.progressEvents += 1;

      // Wait for onRows to process (backpressure)
      const result = onRows?.({ report, facility, page: pageNumber, lastPage: effectivePageCap, rows, runId });
      if (result && typeof result.then === 'function') {
        await result;
      }

      pagesProcessed++;

      // Dynamic yielding based on dataset size
      const yieldInterval = backpressureConfig?.yieldEvery || YIELD_EVERY_N_PAGES;
      const alwaysYield = (stopAtPage || declaredLastPage) >= (pipelineOptions.yieldPageThreshold ?? YIELD_EVERY_PAGE_THRESHOLD);
      if (alwaysYield || pagesProcessed % yieldInterval === 0) {
        await performYield();
      }

      // Ensure payload rows are released ASAP
      payload.rows = null;
    };

    try {
      instrumentation.recordRequest(1);
      const firstFetchStarted = performance.now();
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
      }), { onTransient: (err) => classifyError(err).transient, report }).finally(() => {
        instrumentation.recordRequest(-1);
      });

      latencyTracker.push(report, performance.now() - firstFetchStarted);
      declaredLastPage = Math.max(1, Number(firstPayload?.last_page || 1));
      effectiveLastPage = declaredLastPage;
      backpressureConfig = getBackpressureConfig(declaredLastPage);
      scheduler.setBackpressureCeiling(backpressureConfig?.maxInFlight);
      if (declaredLastPage > 200) {
        onWarning?.(`Large dataset detected (${declaredLastPage} pages). Using conservative backpressure: max ${backpressureConfig.maxInFlight} concurrent, yield every ${backpressureConfig.yieldEvery} pages.`);
      }

      const fetchBufferCapBase = pipelineOptions.fetchBufferMax ?? deriveInitialFetchCap(declaredLastPage);
      const processingCapBase = pipelineOptions.processingPoolMax ?? deriveInitialProcessingCap(declaredLastPage);
      // Use pageQueueLimit from backpressure config
      const effectiveQueueLimit = bpConfig.pageQueueLimit;
      let fetchBufferCap = Math.max(FETCH_BUFFER_MAX_MIN, Math.min(fetchBufferCapBase, effectiveQueueLimit));
      let processingCap = Math.max(PROCESSING_POOL_MAX_MIN, Math.min(processingCapBase, PROCESSING_POOL_MAX_HARD));
      let lastLatencyAdjust = 0;
      let lastMemoryCheck = 0;

      const fetchBuffer = [];
      const inflightFetches = new Set();
      const activeProcessingTasks = new Set();
      let completedProcessingCount = 0;
      let nextPage = 2;

      const applyLatencyAdjustments = () => {
        const now = performance.now();
        if (now - lastLatencyAdjust < LATENCY_ADJUST_COOLDOWN_MS) return;
        const p90 = latencyTracker.p90(report);
        if (!p90) return;

        if (p90 > (LATENCY_TARGETS?.p90SpikeMs || Infinity)) {
          if (fetchBufferCap > FETCH_BUFFER_MAX_MIN) {
            const nextCap = Math.max(FETCH_BUFFER_MAX_MIN, Math.floor(fetchBufferCap * 0.7));
            fetchBufferCap = nextCap;
            lastLatencyAdjust = now;
            emitPipelinePerf({ reason: 'latency_backoff', fetchBufferCap, processingCap });
          } else if (processingCap > PROCESSING_POOL_MAX_MIN) {
            processingCap = Math.max(PROCESSING_POOL_MAX_MIN, processingCap - 1);
            lastLatencyAdjust = now;
            emitPipelinePerf({ reason: 'latency_processing_backoff', fetchBufferCap, processingCap });
          }
        } else if (p90 < (LATENCY_TARGETS?.p90RecoverMs || 0)) {
          let changed = false;
          if (fetchBufferCap < fetchBufferCapBase) {
            fetchBufferCap = Math.min(fetchBufferCapBase, fetchBufferCap + 1);
            changed = true;
          }
          if (processingCap < processingCapBase && !changed) {
            processingCap = Math.min(processingCapBase, processingCap + 1);
            changed = true;
          }
          if (changed) {
            lastLatencyAdjust = now;
            emitPipelinePerf({ reason: 'latency_recover', fetchBufferCap, processingCap });
          }
        }
      };

      const maybeAdjustForMemory = () => {
        if (typeof performance === 'undefined' || !performance.memory) return;
        const now = performance.now();
        if (now - lastMemoryCheck < MEMORY_CHECK_INTERVAL_MS) return;
        lastMemoryCheck = now;
        const { usedJSHeapSize, jsHeapSizeLimit } = performance.memory;
        if (!jsHeapSizeLimit || !usedJSHeapSize) return;
        const ratio = usedJSHeapSize / jsHeapSizeLimit;
        if (ratio > MEMORY_PRESSURE_THRESHOLD) {
          if (fetchBufferCap > FETCH_BUFFER_MAX_MIN) {
            fetchBufferCap = Math.max(FETCH_BUFFER_MAX_MIN, fetchBufferCap - 1);
            emitPipelinePerf({ reason: 'memory_backoff', fetchBufferCap, processingCap });
          } else if (processingCap > PROCESSING_POOL_MAX_MIN) {
            processingCap = Math.max(PROCESSING_POOL_MAX_MIN, processingCap - 1);
            emitPipelinePerf({ reason: 'memory_processing_backoff', fetchBufferCap, processingCap });
          }
        } else if (ratio < MEMORY_PRESSURE_RECOVER) {
          let updated = false;
          if (fetchBufferCap < fetchBufferCapBase) {
            fetchBufferCap = Math.min(fetchBufferCapBase, fetchBufferCap + 1);
            updated = true;
          }
          if (!updated && processingCap < processingCapBase) {
            processingCap = Math.min(processingCapBase, processingCap + 1);
            updated = true;
          }
          if (updated) {
            emitPipelinePerf({ reason: 'memory_recover', fetchBufferCap, processingCap });
          }
        }
      };

      const schedulePump = (() => {
        let scheduled = false;
        return () => {
          if (scheduled) return;
          scheduled = true;
          setTimeout(() => {
            scheduled = false;
            pump();
          }, 0);
        };
      })();

      const normalizePayload = (payload, pageNumber) => {
        if (!payload) return null;
        const rows = Array.isArray(payload.data) ? payload.data : [];
        if (typeof payload.last_page === 'number') {
          declaredLastPage = Math.max(declaredLastPage, payload.last_page || pageNumber);
        }
        if (!payload.next_page_url) {
          stopAtPage = stopAtPage ? Math.min(stopAtPage, pageNumber) : pageNumber;
        }
        effectiveLastPage = stopAtPage || declaredLastPage;
        const compactPayload = {
          pageNumber,
          rows,
          last_page: payload.last_page ?? declaredLastPage,
          next_page_url: payload.next_page_url ?? null,
        };
        const weight = estimatePayloadWeight(rows);
        compactPayload.payloadBytes = weight.approxBytes;
        compactPayload.fieldCount = weight.fieldCount;
        compactPayload.rowCount = weight.rowCount;
        return compactPayload;
      };

      const fetchBufferState = () => ({
        fetchBufferLength: fetchBuffer.length,
        fetchInFlight: inflightFetches.size,
        processingActive: activeProcessingTasks.size,
      });

      const maybeEmitSnapshot = () => {
        const state = fetchBufferState();
        updateStats(state);
        emitPipelinePerf({
          fetchBuffer: state.fetchBufferLength,
          fetchInFlight: state.fetchInFlight,
          processingActive: state.processingActive,
          fetchBufferCap,
          processingCap,
        });
      };

      const scheduleProcessing = () => {
        while (activeProcessingTasks.size < processingCap && fetchBuffer.length) {
          const payload = fetchBuffer.shift();
          const processTask = (async () => {
            try {
              await handlePage(payload, payload.pageNumber);
              completedProcessingCount++;
            } catch (err) {
              markError(err);
              throw err;
            } finally {
              activeProcessingTasks.delete(processTask);
              maybeEmitSnapshot();
              schedulePump();
            }
          })();
          activeProcessingTasks.add(processTask);
          maybeEmitSnapshot();
        }
      };

      const pump = () => {
        if (pipelineError) return;
        if (runIdCheck && runIdCheck() !== runId) return;
        if (signal?.aborted) return;

        maybeAdjustForMemory();
        const targetLastPage = stopAtPage || declaredLastPage;
        const remainingToFetch = Math.max(0, targetLastPage - (nextPage - 1));
        const desiredFetchSlots = Math.min(fetchBufferCap, effectiveQueueLimit, remainingToFetch || fetchBufferCap);

        while (
          activeProcessingTasks.size + fetchBuffer.length < fetchBufferCap + processingCap &&
          inflightFetches.size + fetchBuffer.length < desiredFetchSlots &&
          nextPage <= targetLastPage
        ) {
          const pageNumber = nextPage++;
          instrumentation.recordQueuedTask(1);
          instrumentation.recordRequest(1);
          const startedAt = performance.now();
          const fetchPromise = scheduler.enqueue(() => fetchReportPage({
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
          }), { onTransient: (err) => classifyError(err).transient, report });

          const fetchTask = fetchPromise
            .then((payload) => {
              instrumentation.recordRequest(-1);
              latencyTracker.push(report, performance.now() - startedAt);
              applyLatencyAdjustments();
              if (runIdCheck && runIdCheck() !== runId) return null;
              return normalizePayload(payload, pageNumber);
            })
            .then((compact) => {
              if (compact) {
                fetchBuffer.push(compact);
              }
              inflightFetches.delete(fetchTask);
              instrumentation.recordQueuedTask(-1);
              maybeEmitSnapshot();
              scheduleProcessing();
            })
            .catch((err) => {
              instrumentation.recordRequest(-1);
              instrumentation.recordQueuedTask(-1);
              inflightFetches.delete(fetchTask);
              markError(err);
              schedulePump();
              throw err;
            });

          inflightFetches.add(fetchTask);
          maybeEmitSnapshot();
        }

        scheduleProcessing();
      };

      // Process first page immediately using the new pipeline stats helpers
      await handlePage(normalizePayload(firstPayload, 1), 1);
      completedProcessingCount = 1;
      maybeEmitSnapshot();

      pump();

      // Wait for all work to complete using a loop that checks remaining work
      while (completedProcessingCount < (stopAtPage || effectiveLastPage)) {
        if (runIdCheck && runIdCheck() !== runId) break;
        if (signal?.aborted) break;
        if (pipelineError) break;

        const waitSet = new Set([...inflightFetches, ...activeProcessingTasks]);
        if (waitSet.size === 0) {
          pump();
          if (!fetchBuffer.length && !inflightFetches.size && !activeProcessingTasks.size) break;
          continue;
        }

        try {
          await Promise.race(waitSet);
        } catch {
          // Errors are recorded via pipelineError
        }
        await performYield();
      }

      facilityStats.finalBuffer = fetchBuffer.length;
      facilityStats.finalProcessing = activeProcessingTasks.size;

      if (pipelineError) throw pipelineError;

      if (!signal?.aborted && (!runIdCheck || runIdCheck() === runId)) {
        onFacilityStatus?.({ report, facility, status: 'done' });
      }
      return { report, facility, stats: facilityStats };
    } catch (e) {
      if (signal?.aborted || (runIdCheck && runIdCheck() !== runId)) {
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

    let currentRunId = runId;
    const runIdCheck = () => currentRunId;

    const tasks = [];
    for (const report of reports) {
      for (const facility of facilities) {
        tasks.push(processReportFacility({
          report,
          facility,
          startDate,
          endDate,
          timezone,
          onRows,
          runIdCheck
        }));
      }
    }

    try {
      return await Promise.all(tasks);
    } catch (err) {
      scheduler.cancel('failure');
      throw err;
    }
  }

  function cancel() {
    scheduler.cancel('cancelled');
  }

  return { run, cancel, runId };
}
