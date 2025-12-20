# YMS QBR Assistant

A static, web-based app for Customer Experience + Sales to quantify customer adoption, operational outcomes, and ROI signals using YMS report data pulled from an API.

- **Hosting:** GitHub Pages (static files only)
- **Stack:** Plain HTML/CSS/JS (ES Modules), **Chart.js** + **Luxon** via CDN
- **Security:** API token is **in-memory only** (never persisted)
- **PII policy:** **Driver phone/cell values are never displayed or exported** (hard-scrubbed by key name)
- **Performance:** Optional Web Worker keeps the UI responsive during heavy aggregation (auto-fallback), plus adaptive concurrency + UI throttling for long runs.

---

## Project structure

index.html # Layout + CDN scripts + module entry
styles.css # YMS-branded UI + print styles
app.js # UI controller/state + orchestration
api.js # Fetching, pagination, retry, concurrency, cancellation
analysis.js # Streaming aggregations + findings + data quality scoring
charts.js # Chart.js rendering + PNG export + chart-data CSV helpers
export.js # Summary TXT, CSV export utilities, print helpers
mock-data.js # Small embedded sample payloads for Mock mode
README.md
AGENTS.md # AI navigation + invariants (for Codex/agents)

---

## How the app works

### User enters:
- Tenant (client subdomain)
- API token (masked; in-memory only)
- Facility codes
- Date range
- Timezone (IANA)
- Report selections

### On “Run Assessment” the app fetches every selected report for every facility, using pagination:
- Request page 1 first to learn `last_page`
- Fan-out pages 2..last_page into a global adaptive concurrency pool (start 8, ramps toward 20 when healthy) with per-report lane caps so slow endpoints (e.g., driver_history) back off first
- Progress updates are throttled (~5x/sec) and chart rendering is throttled (~1s cadence) to keep the main thread responsive
- After each page: stream rows into aggregators and discard raw rows

### The UI shows:
- Per-report & per-facility progress
- Metrics, charts, findings, recommendations, and confidence indicators

### Exports:
- Summary TXT
- Print-to-PDF (via window.print())
- Per-chart PNG export
- Per-chart aggregated CSV export + per-report aggregated summary CSV export

---

## Web Worker mode (optional)

- Toggle “Use Web Worker (recommended)” in the Inputs panel to offload normalization + aggregation off the main thread.
- The worker never receives the API token—only raw rows for the selected page are sent, and phone/cell fields are still scrubbed immediately.
- If the browser cannot start a worker, the app automatically falls back to the main-thread analysis path.

---

## Reports:
- current_inventory
- detention_history
- dockdoor_history
- driver_history
- trailer_history

---

## Timezones & timestamp parsing

Most API timestamps are treated as UTC, then converted to the selected timezone for:
- grouping by day/week/month
- chart axis labels
- displayed summaries

### Special exception

A field named timezone_arrival_time is already in facility local time.
- Do not convert it “from UTC”
- Treat it as local time
- If charted, label as “local facility time”

### Parsing robustness

Reports may contain:
- YYYY-MM-DD HH:mm:ss
- MM-DD-YYYY HH:mm
- separate date + time fields

Parsing is centralized (Luxon-based). The app tracks:
- parse success rate
- coverage % for required timestamp pairs
- “data quality” score per report

Warnings surface parsing failures and suspicious null patterns.

---

## Mock mode

A “Mock mode” toggle allows demoing without hitting the live API.
- Uses small embedded JSON payloads in mock-data.js
- Still runs through the same aggregation + chart + export pipeline
- Useful for sales enablement demos and UI iteration

---

## Extending the app

### Add a new report
- Add report name to the UI list in app.js.
- Add a streaming aggregator in analysis.js.
- Add chart definitions + dataset builders in charts.js.
- Add findings/recommendations + confidence rules in analysis.js.
- Ensure scrubber is applied to all rows before analysis/export.

### Adjust concurrency / retries
Update defaults in api.js:
- `CONCURRENCY_MIN`, `CONCURRENCY_START`, `CONCURRENCY_MAX` (global pool across all report/facility pages). Defaults now bias to a higher start (8) and allow a higher ceiling (20) for large runs.
- `PER_REPORT_LIMITS` caps lanes per report (`driver_history` defaults to max 6, others default max 18) so slower endpoints do not poison the global pool.
- `LATENCY_TARGETS` drive lane-level backoff/recovery using p90 latency (spike threshold 2.6s, recover threshold 1.7s) with jittered retries.
- `RETRY_LIMIT`, `BACKOFF_BASE_MS`, `BACKOFF_JITTER`
- `DEFAULT_TIMEOUT_MS` (base request timeout, defaults to 60s)
- `SLOW_FIRST_PAGE_TIMEOUT_MS` (extended timeout for slow first-page endpoints like `driver_history`, defaults to 90s)
- Adaptive ramp-up occurs after sustained successes; transient errors reduce concurrency automatically. Lane-level backoff happens before global backoff when a single report slows down.

### Performance behaviors
- Fan-out pagination: page 1 first, then enqueue remaining pages into a shared scheduler with lane-aware caps.
- Adaptive concurrency: starts at 8, ramps up to 20 globally when healthy, and backs off globally on transient errors. Per-report lanes (e.g., `driver_history`) back off first when their p90 latency spikes, recovering independently when latency improves.
- Retry/backoff: transient failures (timeouts, 408/429/5xx, network errors) retry up to `RETRY_LIMIT` with abort-aware exponential backoff + jitter.
- UI throttling: progress updates limited to ~5/sec; chart re-renders throttled to ~1.2s to avoid main-thread thrash on large runs; perf panel throttled to ~0.9s when enabled.
- Timestamp parsing prefers fast-path parsing for common formats before falling back to Luxon, reducing per-row allocations on very large runs.
- Streaming quantiles (P² estimator) remain in use for median/p90 without storing raw arrays.

### Perf instrumentation (optional)
- Append `?perf=1` to enable lightweight instrumentation. A “Performance (debug)” card shows:
  - request latency p50/p90 per report lane
  - rows/sec and ms/row processing (main-thread path)
  - total chart render time
- No tokens or PII are logged or displayed; instrumentation is in-memory and easy to remove.

### Developer notes (quick start for maintainers)
- Key modules changed:
  - `api.js`: global adaptive request scheduler, fan-out pagination, abort-aware retry/backoff.
  - `app.js`: throttled progress/charts, adaptive concurrency notifications, stricter cancellation/token wiping.
  - `analysis.js`: fast-path timestamp parsing for common formats before falling back to Luxon.
- Quick tests in Mock mode: enable “Mock mode”, select multiple facilities/reports, run—watch throttled progress and final charts.
- Cancel test: start a mock run, click “Cancel run”; progress should stop quickly, banner updates, token is cleared.
- 401/403 test: use an obviously bad token in live mode; run should stop with “Invalid token/not authorized” messaging and queued tasks cancelled.
- PII guardrails: phone/cell keys are scrubbed at normalization; exports exclude them by design.

### Add new export formats
Implement in export.js and wire buttons in app.js.
