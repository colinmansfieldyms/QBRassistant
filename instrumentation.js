/**
 * Lightweight instrumentation for diagnosing browser unresponsiveness.
 * Enabled via ?debug=1 in URL.
 *
 * ROOT CAUSE ANALYSIS:
 * ====================
 * The browser freeze occurs due to:
 * 1. Main-thread saturation: Too many pages (10-60) fetched concurrently
 * 2. Synchronous processing: handlePage() awaits onRows() which processes all rows synchronously
 * 3. No yielding: Event loop never gets control back during heavy processing
 * 4. Memory pressure: All page data held in memory until processing completes
 * 5. UI update storms: Progress/render updates fire per-page without batching
 *
 * FIX STRATEGY:
 * =============
 * - Reduce PAGE_QUEUE_LIMIT from 60→10 (done) and add dynamic backpressure
 * - Yield to event loop after every N pages processed
 * - Batch all UI updates (progress, charts) with coalescing
 * - Stream/aggregate: process rows incrementally, discard page data immediately
 * - Add runId/generation token for cancellation correctness
 */

const DEBUG_ENABLED = new URLSearchParams(window.location.search).has('debug');

export class Instrumentation {
  constructor() {
    this.enabled = DEBUG_ENABLED;
    this.startTime = null;
    this.metrics = {
      inFlightRequests: 0,
      queuedTasks: 0,
      completedPages: 0,
      bytesReceived: 0,
      parseTimeMs: 0,
      analysisTimeMs: 0,
      renderTimeMs: 0,
      mainThreadLongTasks: 0,
      yieldCount: 0,
      batchedUpdates: 0,
    };
    this.longTaskThresholdMs = 50; // Tasks > 50ms are considered "long"
    this.lastHeartbeat = performance.now();
    this.heartbeatInterval = null;
    this.logThrottleTimer = null;
    this.perfObserver = null;

    if (this.enabled) {
      this.init();
    }
  }

  init() {
    this.startTime = performance.now();

    // PerformanceObserver for longtask if available
    if (typeof PerformanceObserver !== 'undefined') {
      try {
        this.perfObserver = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            if (entry.duration > this.longTaskThresholdMs) {
              this.metrics.mainThreadLongTasks++;
            }
          }
        });
        this.perfObserver.observe({ entryTypes: ['longtask'] });
      } catch (e) {
        // longtask not supported, use fallback
        this.startHeartbeatMonitor();
      }
    } else {
      this.startHeartbeatMonitor();
    }

    // Periodic logging (throttled)
    this.scheduleLog();
  }

  startHeartbeatMonitor() {
    // Fallback: detect event loop stalls via setInterval drift
    this.heartbeatInterval = setInterval(() => {
      const now = performance.now();
      const drift = now - this.lastHeartbeat - 100; // Expected 100ms interval
      if (drift > this.longTaskThresholdMs) {
        this.metrics.mainThreadLongTasks++;
      }
      this.lastHeartbeat = now;
    }, 100);
  }

  scheduleLog() {
    if (this.logThrottleTimer) return;
    this.logThrottleTimer = setTimeout(() => {
      this.logThrottleTimer = null;
      this.logSummary();
      if (this.enabled) this.scheduleLog();
    }, 2000); // Log every 2s
  }

  logSummary() {
    if (!this.enabled) return;
    const elapsed = ((performance.now() - this.startTime) / 1000).toFixed(1);
    console.log(`[Instrumentation @ ${elapsed}s]`, {
      ...this.metrics,
      elapsedSec: elapsed,
      avgPageTimeMs: this.metrics.completedPages > 0
        ? ((this.metrics.parseTimeMs + this.metrics.analysisTimeMs) / this.metrics.completedPages).toFixed(1)
        : 0,
    });
  }

  recordRequest(delta) {
    this.metrics.inFlightRequests += delta;
  }

  recordQueuedTask(delta) {
    this.metrics.queuedTasks += delta;
  }

  recordPageComplete(bytes) {
    this.metrics.completedPages++;
    this.metrics.bytesReceived += bytes || 0;
  }

  recordParse(ms) {
    this.metrics.parseTimeMs += ms;
  }

  recordAnalysis(ms) {
    this.metrics.analysisTimeMs += ms;
  }

  recordRender(ms) {
    this.metrics.renderTimeMs += ms;
  }

  recordYield() {
    this.metrics.yieldCount++;
  }

  recordBatchedUpdate() {
    this.metrics.batchedUpdates++;
  }

  destroy() {
    if (this.perfObserver) {
      this.perfObserver.disconnect();
      this.perfObserver = null;
    }
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    if (this.logThrottleTimer) {
      clearTimeout(this.logThrottleTimer);
      this.logThrottleTimer = null;
    }
  }

  reset() {
    this.startTime = performance.now();
    this.metrics = {
      inFlightRequests: 0,
      queuedTasks: 0,
      completedPages: 0,
      bytesReceived: 0,
      parseTimeMs: 0,
      analysisTimeMs: 0,
      renderTimeMs: 0,
      mainThreadLongTasks: 0,
      yieldCount: 0,
      batchedUpdates: 0,
    };
    this.lastHeartbeat = performance.now();
  }
}

// Global instance
export const instrumentation = new Instrumentation();
