# YMS QBR Assistant

A static, web-based app for Customer Experience + Sales to quantify customer adoption, operational outcomes, and ROI signals using YMS report data pulled from an API.

- **Hosting:** GitHub Pages (static files only)
- **Stack:** Plain HTML/CSS/JS (ES Modules), **Chart.js** + **Luxon** via CDN
- **Security:** API token is **in-memory only** (never persisted)
- **PII policy:** **Driver phone/cell values are never displayed or exported** (hard-scrubbed by key name)

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
- Request page 1 first to get last_page
- Fetch all pages with retry/backoff + concurrency limit
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
- concurrency limit (e.g., 3–5)
- retries (e.g., 2)
- exponential backoff parameters

### Add new export formats
Implement in export.js and wire buttons in app.js.
