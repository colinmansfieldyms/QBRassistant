export const WORKER_AUTO_THRESHOLD_PAGES = 300;
export const CHUNK_SIZE_DEFAULT = 250;
export const CHUNK_SIZE_MAX = 750;
export const CHUNK_SIZE_STEP = 250;
export const CHUNK_ADJUST_COOLDOWN_MS = 1200;
export const CHUNK_HEADROOM_TARGET_MS = 8;
export const CHUNK_OVERLOAD_TARGET_MS = 18;
export const BACKLOG_HEAVY_PAGES = 6;
export const BACKLOG_LIGHT_PAGES = 1;
export const PARTIAL_EMIT_INTERVAL_MS_DEFAULT = 1000;
export const PARTIAL_EMIT_INTERVAL_MS_HEAVY = 2000;
export const PARTIAL_EMIT_COOLDOWN_MS = 800;
export const RESULTS_RENDER_BASE_MS = 1200;
export const RESULTS_RENDER_MIN_MS = 250;
export const RESULTS_RENDER_MAX_MS = 2400;

export function createAdaptiveState(now = Date.now()) {
  return {
    chunkSize: CHUNK_SIZE_DEFAULT,
    avgChunkMs: 0,
    headroomStreak: 0,
    overloadStreak: 0,
    lastChunkAdjustAt: now,
    partialIntervalMs: PARTIAL_EMIT_INTERVAL_MS_DEFAULT,
    lastPartialAdjustAt: now,
  };
}

export function updateChunkSizing(state, { chunkMs, backlog = 0, now = Date.now() }) {
  const next = { ...state };
  const alpha = 0.25;
  next.avgChunkMs = next.avgChunkMs ? (next.avgChunkMs * (1 - alpha)) + (chunkMs * alpha) : chunkMs;

  const headroom = next.avgChunkMs > 0 && next.avgChunkMs <= CHUNK_HEADROOM_TARGET_MS && backlog <= BACKLOG_LIGHT_PAGES;
  const overloaded = (next.avgChunkMs >= CHUNK_OVERLOAD_TARGET_MS) || (backlog >= BACKLOG_HEAVY_PAGES);

  next.headroomStreak = headroom ? next.headroomStreak + 1 : 0;
  next.overloadStreak = overloaded ? next.overloadStreak + 1 : 0;

  const canAdjust = (now - next.lastChunkAdjustAt) >= CHUNK_ADJUST_COOLDOWN_MS;

  if (canAdjust && next.overloadStreak >= 2 && next.chunkSize > CHUNK_SIZE_DEFAULT) {
    next.chunkSize = Math.max(CHUNK_SIZE_DEFAULT, next.chunkSize - CHUNK_SIZE_STEP);
    next.lastChunkAdjustAt = now;
    next.headroomStreak = 0;
    next.overloadStreak = 0;
  } else if (canAdjust && next.headroomStreak >= 3 && next.chunkSize < CHUNK_SIZE_MAX) {
    next.chunkSize = Math.min(CHUNK_SIZE_MAX, next.chunkSize + CHUNK_SIZE_STEP);
    next.lastChunkAdjustAt = now;
    next.headroomStreak = 0;
    next.overloadStreak = 0;
  }

  return next;
}

export function updatePartialInterval(state, { backlog = 0, chunkSize, now = Date.now() }) {
  const next = { ...state };
  const heavy = backlog >= BACKLOG_HEAVY_PAGES || chunkSize >= CHUNK_SIZE_MAX;
  const canAdjust = (now - next.lastPartialAdjustAt) >= PARTIAL_EMIT_COOLDOWN_MS;

  if (heavy && canAdjust && next.partialIntervalMs < PARTIAL_EMIT_INTERVAL_MS_HEAVY) {
    next.partialIntervalMs = PARTIAL_EMIT_INTERVAL_MS_HEAVY;
    next.lastPartialAdjustAt = now;
  } else if (!heavy && canAdjust && next.partialIntervalMs > PARTIAL_EMIT_INTERVAL_MS_DEFAULT) {
    next.partialIntervalMs = PARTIAL_EMIT_INTERVAL_MS_DEFAULT;
    next.lastPartialAdjustAt = now;
  }

  return next;
}

export function shouldAutoUseWorker({ estimatedPages, threshold = WORKER_AUTO_THRESHOLD_PAGES, workerAvailable, preferred }) {
  if (!workerAvailable) return false;
  if (typeof estimatedPages === 'number' && estimatedPages >= threshold) return true;
  return !!preferred;
}

export function computeRenderThrottle(partialIntervalMs) {
  const target = Math.max(PARTIAL_EMIT_INTERVAL_MS_DEFAULT, partialIntervalMs || 0);
  return Math.max(RESULTS_RENDER_MIN_MS, Math.min(target, RESULTS_RENDER_MAX_MS));
}

