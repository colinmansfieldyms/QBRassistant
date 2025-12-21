/**
 * Time estimation module for tracking API processing speed and calculating ETA.
 * Uses exponential moving average for smooth, responsive estimates.
 */

// Exponential moving average weight (0.3 = responsive to changes, 0.1 = more stable)
const EMA_ALPHA = 0.25;

// Minimum samples before showing estimate
const MIN_SAMPLES_FOR_ESTIMATE = 3;

// Update frequency for ETA calculations (avoid recalculating too often)
const RECALC_THROTTLE_MS = 500;

export function createETATracker() {
  let startTime = null;
  let totalPages = 0;
  let completedPages = 0;
  let emaPageTimeMs = null;  // Exponential moving average of time per page
  let lastPageCompletedAt = null;
  let lastRecalcAt = 0;
  let cachedETA = null;

  // Track per-report/facility progress for more granular estimates
  const taskProgress = new Map(); // key: "report/facility" -> { completed, total, avgMs }

  function reset() {
    startTime = null;
    totalPages = 0;
    completedPages = 0;
    emaPageTimeMs = null;
    lastPageCompletedAt = null;
    lastRecalcAt = 0;
    cachedETA = null;
    taskProgress.clear();
  }

  function start() {
    reset();
    startTime = performance.now();
    lastPageCompletedAt = startTime;
  }

  function setTotalPages(report, facility, pages) {
    const key = `${report}/${facility}`;
    const existing = taskProgress.get(key) || { completed: 0, total: 0, samples: [] };
    existing.total = pages;
    taskProgress.set(key, existing);

    // Recalculate total
    totalPages = 0;
    for (const task of taskProgress.values()) {
      totalPages += task.total;
    }
  }

  function recordPageComplete(report, facility) {
    const now = performance.now();
    const key = `${report}/${facility}`;
    const task = taskProgress.get(key) || { completed: 0, total: 0, samples: [] };

    // Calculate time for this page
    const pageTime = lastPageCompletedAt ? (now - lastPageCompletedAt) : null;
    lastPageCompletedAt = now;

    task.completed++;
    completedPages++;

    // Only use reasonable page times (ignore first page, outliers)
    if (pageTime !== null && pageTime > 0 && pageTime < 30000) {
      task.samples.push(pageTime);
      // Keep only last 20 samples per task
      if (task.samples.length > 20) task.samples.shift();

      // Update EMA
      if (emaPageTimeMs === null) {
        emaPageTimeMs = pageTime;
      } else {
        emaPageTimeMs = EMA_ALPHA * pageTime + (1 - EMA_ALPHA) * emaPageTimeMs;
      }
    }

    taskProgress.set(key, task);

    // Invalidate cache
    cachedETA = null;
  }

  function getEstimate() {
    const now = performance.now();

    // Throttle recalculation
    if (cachedETA !== null && (now - lastRecalcAt) < RECALC_THROTTLE_MS) {
      return cachedETA;
    }
    lastRecalcAt = now;

    // Not enough data yet
    if (completedPages < MIN_SAMPLES_FOR_ESTIMATE || emaPageTimeMs === null) {
      cachedETA = {
        ready: false,
        completedPages,
        totalPages,
        percentComplete: totalPages > 0 ? Math.round((completedPages / totalPages) * 100) : 0,
        elapsedMs: startTime ? (now - startTime) : 0,
        remainingMs: null,
        remainingText: null,
        pagesPerSecond: null,
      };
      return cachedETA;
    }

    const remainingPages = Math.max(0, totalPages - completedPages);
    const remainingMs = remainingPages * emaPageTimeMs;
    const elapsedMs = startTime ? (now - startTime) : 0;
    const pagesPerSecond = elapsedMs > 0 ? (completedPages / elapsedMs) * 1000 : 0;

    cachedETA = {
      ready: true,
      completedPages,
      totalPages,
      percentComplete: totalPages > 0 ? Math.round((completedPages / totalPages) * 100) : 0,
      elapsedMs,
      remainingMs,
      remainingText: formatRemainingTime(remainingMs),
      pagesPerSecond: Math.round(pagesPerSecond * 10) / 10,
      avgPageTimeMs: Math.round(emaPageTimeMs),
    };

    return cachedETA;
  }

  function getProgress() {
    return {
      completed: completedPages,
      total: totalPages,
      tasks: Object.fromEntries(taskProgress),
    };
  }

  return {
    reset,
    start,
    setTotalPages,
    recordPageComplete,
    getEstimate,
    getProgress,
  };
}

/**
 * Format remaining time in human-readable form
 */
export function formatRemainingTime(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) {
    return null;
  }

  const seconds = Math.ceil(ms / 1000);

  if (seconds < 5) {
    return 'Almost done';
  }

  if (seconds < 60) {
    return `~${seconds} sec remaining`;
  }

  const minutes = Math.round(seconds / 60);
  if (minutes === 1) {
    return '~1 min remaining';
  }

  if (minutes < 60) {
    return `~${minutes} min remaining`;
  }

  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;

  if (hours === 1) {
    if (remainingMinutes === 0) {
      return '~1 hour remaining';
    }
    return `~1 hr ${remainingMinutes} min remaining`;
  }

  if (remainingMinutes === 0) {
    return `~${hours} hours remaining`;
  }

  return `~${hours} hr ${remainingMinutes} min remaining`;
}

// Global instance for easy access
export const etaTracker = createETATracker();
