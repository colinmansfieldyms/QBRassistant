# AGENTS.md — Codebase Navigation & Invariants (for AI tools)

This file is for AI coding agents (e.g., ChatGPT CODEX) to quickly understand the repo, make safe edits, and navigate without breaking the app’s security/PII constraints.

If you modify anything involving **token handling**, **PII scrubbing**, **exports**, or **timezone logic**, re-check the **Invariants** section before finalizing changes.

---

## Mission

A static, GitHub Pages–hostable web app that:
- Fetches paginated YMS reports across multiple facilities
- Streams rows into aggregations (avoid storing raw data by default)
- Produces metrics, charts, findings, recommendations, and confidence indicators
- Exports Summary TXT, print-to-PDF, chart PNG, and aggregated CSV
- Keeps API token **in-memory only**
- Enforces strict PII handling (never show/export phone/cell values)

---

## Invariants (do not violate)

### 1) Security: token handling (non-negotiable)
- Token must **never** be persisted:
  - No localStorage / sessionStorage / IndexedDB / cookies
- Token must **never** be placed in the URL
- Token must **never** be logged (console, telemetry, debug views)
- “Clear token now” must:
  - wipe the input field
  - null out any in-memory token reference
  - abort all in-flight requests
- Cancel and completion must also:
  - abort requests
  - null token references
- Debug mode must not print token or headers.

> Note: Browser DevTools Network can show Authorization headers during live requests. That’s normal browser behavior; the app must simply not retain the token after run/cancel/clear.

### 2) PII policy: phone/cell data must never be displayed or exported
- Any field name containing `cell` or `phone` must be **dropped during normalization** before any metric/chart/export.
- CSV exports must also exclude these keys (defense in depth).
- You may only use **presence/absence** (null vs not null) for metrics (e.g., “texting could be in use”).

### 3) Performance model: streaming aggregation
- Default behavior must be:
  - Fetch page → update metrics/aggregates → discard rows
- Do not store giant raw datasets unless there is an explicit user action/feature enabling it (MVP defaults to no raw retention).

### 4) Timezone correctness
- Most timestamps are **UTC** → convert to selected timezone for:
  - grouping (day/week/month)
  - chart labels
  - displayed date summaries
- Exception: `timezone_arrival_time` is already facility local time:
  - Do **not** treat it as UTC
  - Treat as local time
  - Label clearly if charted (“local facility time”).

### 5) Resilience
- Pagination must fetch all pages.
- Retry transient errors with exponential backoff (small retry count, e.g., 2).
- Concurrency limit must cap overall page fetches (e.g., 3–5).
- Cancellation must abort quickly via AbortController (including aborting waits/backoff if implemented).

---

## File map (where things live)

### `index.html`
- Static UI skeleton:
  - Inputs panel (tenant, token, facilities, date range, timezone, report checkboxes)
  - Run / Cancel
  - Progress panel
  - Results dashboard
  - Export panel
- Loads CDN libs (Chart.js + Luxon) and app entry `app.js` via `type="module"`.

### `styles.css`
- YMS brand palette:
  - Accent: `#262262`
  - Highlight: `#F9CB3B`
  - Backgrounds: `#F7F7F7` / white
- Responsive layout + print CSS (print-to-PDF).

### `app.js` — UI controller / orchestrator
Responsibilities:
- Owns state:
  - selected reports, facilities, timezone, date range, assumptions
- Reads token from masked input into memory for the duration of a run
- Wires UI events:
  - Run, Cancel, Clear Token, Reset All
  - Export actions (TXT, Print, CSV, PNG)
- Calls:
  - `api.js` for fetching/pagination
  - `analysis.js` for streaming aggregation + findings
  - `charts.js` to render charts from aggregated data
  - `export.js` to produce TXT/CSV/print behaviors
- Updates progress UI:
  - per report, per facility, pages fetched X/Y

High-risk areas:
- token lifecycle
- passing scrubbed vs raw rows into analysis/export
- cancellation state

### `api.js` — network/pagination/retry/concurrency/abort
Responsibilities:
- Build URLs using `URLSearchParams`
- Set headers on every request (including GET):
  - `Accept: application/json`
  - `Content-Type: application/json`
  - `Authorization: Bearer <token>`
- Implement:
  - page fetch with abort support
  - pagination loop (page 1 first → read `last_page` → fetch remaining)
  - global concurrency limiter for page fetches
  - retry/backoff for transient errors
- Must invoke `onProgress({ report, facility, page, lastPage, rowsProcessed })` after each page.

Rules:
- Never log headers or token
- Bubble 401/403 distinctly so UI can show “invalid token”.

### `analysis.js` — streaming aggregations/findings/data quality
Responsibilities:
- Centralized **field scrubber**:
  - drop any keys containing `cell` or `phone` (case-insensitive)
- Centralized timestamp parsing using Luxon:
  - supports multiple formats
  - tracks parse success/failure rates
- Streaming aggregators per report:
  - update compact state (counts, buckets, series)
  - avoid storing raw rows
- Data quality scoring per report:
  - coverage %
  - parse success %
  - suspicious null patterns
- Findings & recommendations:
  - red/yellow/green flags
  - confidence indicator & text

When adding metrics:
- Ensure scrubber is applied before using row fields
- Avoid collecting large arrays; prefer histograms/buckets/rolling stats.

### `charts.js` — Chart.js rendering + PNG export + chart-data extraction for CSV
Responsibilities:
- Create/destroy/update Chart.js instances
- At least 2 charts per report
- Provide:
  - per-chart “Download PNG” using `canvas.toDataURL()`
  - underlying aggregated dataset for CSV export (“chart data as CSV”)
- Do not use raw rows for charts; only aggregated series/tables.

### `export.js` — TXT + CSV + print helpers
Responsibilities:
- Generate Summary TXT:
  - key metrics per report
  - findings + confidence
  - assumptions used
  - timezone and date range metadata
- CSV exports:
  - chart-level aggregated series/table
  - per-report aggregated summary
  - include metadata header rows (timezone/date range)
  - enforce PII scrub (exclude `cell`/`phone` keys) as a second line of defense
- Print-to-PDF:
  - `window.print()`
  - relies on print CSS for a clean report layout.

### `mock-data.js`
- Contains small, fake sample payloads that mimic API pagination:
  - `current_page`, `last_page`, `next_page_url`, `data`
- Enables “Mock mode” for demos without hitting the API.
- Must not contain any real customer data or secrets.

---

## Common agent tasks (where to edit)

### Add a new report
1. `app.js`: add report option + wiring
2. `api.js`: ensure report name is valid and routed through fetch pipeline
3. `analysis.js`: create streaming aggregator + data quality rules
4. `charts.js`: add charts + dataset export helpers
5. `export.js`: include report summary in TXT/CSV exports
6. Verify scrubber covers any new fields

### Add a new chart to an existing report
- `analysis.js`: produce the aggregated series/table needed
- `charts.js`: render chart + implement PNG/CSV export
- `app.js`: add UI container and buttons

### Change retry/backoff or concurrency
- `api.js`: adjust limits/policy
- Ensure abort cancels waits/backoff

### Modify timezone handling
- `analysis.js`: grouping boundaries and label conversions
- Keep the `timezone_arrival_time` exception intact

### Improve PII defenses
- Primary: `analysis.js` scrubber (drop keys)
- Secondary: `export.js` scrub checks for CSV/TXT
- Never display raw rows in UI

---

## Safe logging guidelines
Allowed:
- “Starting run”, “Fetched page 3/20”, “Aggregation updated”, “Run cancelled”
Not allowed:
- token or Authorization header
- full raw row dumps
- anything containing `cell` or `phone` keys

---

## Review checklist before submitting changes
- [ ] Token never persisted; cleared on cancel/complete/clear/reset
- [ ] Abort works immediately; no in-flight requests continue after cancel
- [ ] `cell`/`phone` keys are dropped before any analysis/render/export
- [ ] CSV exports are aggregated-only and include timezone/date range metadata
- [ ] Timezone conversions correct; `timezone_arrival_time` treated as local
- [ ] Pagination fetches all pages; progress updates show pages X/Y
- [ ] Concurrency limit respected; retries/backoff applied to transient errors
- [ ] No giant raw datasets stored by default
