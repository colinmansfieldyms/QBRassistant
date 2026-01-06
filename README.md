# YMS QBR Assistant

A web-based tool for analyzing Yard Management Solutions (YMS) data to help Customer Experience and Sales teams quantify customer adoption, operational performance, and return on investment (ROI).

## What does this tool do?

The QBR (Quarterly Business Review) Assistant helps you:

1. **Analyze yard operations** - Understand how efficiently trailers, drivers, and dock doors are being utilized
2. **Identify operational issues** - Surface problems like lost trailers, detention events, and data quality gaps
3. **Calculate ROI estimates** - Quantify the business value of YMS adoption using customer-provided assumptions
4. **Generate reports** - Export findings as charts, CSVs, and printable PDFs for customer presentations

---

## Getting started

### Data input options

The tool supports two ways to load data:

#### CSV Mode (recommended)

Upload CSV files exported from the YMS system. This is the **default and recommended method** for most use cases.

**How to use:**
1. Export reports from YMS as CSV files
2. Drag and drop files into the upload area (or click to browse)
3. The tool auto-detects which report type each file contains
4. Select report type manually if auto-detection fails
5. Enter facility codes and timezone
6. Click "Run Assessment"

**Advantages:**
- Works with any data size - no API timeout concerns
- Large files (50+ MB) are automatically streamed in chunks to prevent memory issues
- No API token required
- Faster for pre-exported data

**Limitations:**
- Detention "prevented" counts may be unavailable (pre-detention timestamps not in CSV exports)
- Date/time values are interpreted in the selected timezone
- Requires manual export from YMS before analysis

#### API Mode

Connect directly to the YMS API for real-time data access. Best for smaller datasets or when fresh data is needed.

**How to use:**
1. Switch to the "API" tab
2. Enter client subdomain (e.g., "acme" for acme.api.ymshub.com)
3. Paste API token (kept in memory only, never saved)
4. Select date range and reports to analyze
5. Enter facility codes and timezone
6. Click "Run Assessment"

**Advantages:**
- Real-time data access
- No manual export step required
- Full detention prevention data available

**Limitations for large datasets:**

| Dataset Size | Behavior |
|--------------|----------|
| Small (< 50 pages) | Normal operation |
| Medium (50-200 pages) | Reduced concurrency, may take several minutes |
| Large (200-500 pages) | Conservative mode, expect 5-15 minutes |
| Very large (500+ pages) | Minimal concurrency, may take 30+ minutes or timeout |

For datasets exceeding ~500 pages, **CSV mode is strongly recommended**. The API has built-in safeguards:
- 60-second timeout per request (90 seconds for initial driver_history page)
- Automatic retry with backoff for transient errors (429, 5xx)
- Memory pressure monitoring - backs off when browser memory is constrained
- Adaptive concurrency based on response latency

### Required inputs

**For CSV mode:**
- **CSV files** - One or more report exports from YMS
- **Facility codes** - One or more facility identifiers (one per line)
- **Timezone** - Used for grouping data by day/week/month

**For API mode:**
- **Tenant** - Client subdomain
- **API token** - Valid authentication token
- **Date range** - Start and end dates for the query
- **Facility codes** - One or more facility identifiers
- **Timezone** - Used for timestamp display and grouping
- **Reports** - Select which reports to analyze

### ROI assumptions (optional)

To enable ROI calculations, provide any of these optional values:

| Assumption | Used for |
|------------|----------|
| Detention cost per hour | Calculating detention-related costs and savings |
| Labor fully loaded rate per hour | Estimating driver labor efficiency and costs |
| Target moves per driver per day | Benchmarking driver performance (default: 50) |
| Target turns per dock door per day | Benchmarking dock door throughput |
| Cost per dock door hour | Calculating dock door idle time costs |

**Refresh ROI** - After running an assessment, you can adjust any ROI assumptions and click the refresh button to recalculate ROI values without re-fetching data. This allows quick "what-if" scenarios with different assumptions.

---

## Available reports

### Current Inventory
Snapshot of trailers currently in the yard, including equipment types, dwell times, and aging analysis.

**CSV-specific feature:** Yard age distribution chart showing trailers in buckets (0-1d, 1-7d, 7-30d, 30d+) based on elapsed time.

### Detention History
Tracks detention events where trailers exceed allowed dwell time. Calculates:
- Total detention hours and events
- Detention costs (when assumptions provided)
- Detention patterns by carrier and time period

**ROI Analysis:** Shows actual detention spend based on recorded detention hours.

### Dock Door History
Analyzes dock door utilization and throughput. Tracks:
- Door occupancy and turns per door
- Loading/unloading patterns
- Idle time analysis

**ROI Analysis:** Compares actual throughput vs target, estimates idle door costs.

### Driver History
Evaluates yard driver productivity. Measures:
- Moves per driver per day
- Active vs idle time
- Performance distribution across drivers

**ROI Analysis:** Compares driver performance to target moves, estimates labor efficiency.

### Trailer History
Tracks trailer events and movements. Identifies:
- Trailer marked lost events (gate check-out accuracy issues)
- Yard check insert events (gate check-in accuracy issues)
- Spot edited events (yard driver YMS usage issues)
- Facility edited events (shuttle/gate/campus operation issues)
- Top carriers by error counts

**ROI Analysis:** Error rate trend analysis showing improvement or degradation over time.

---

## Understanding the results

### Report cards

Each report displays:

1. **Metrics** - Key performance indicators in a grid format
2. **Charts** - Visual trends over time (line charts) or distributions (bar/pie charts)
3. **Findings** - Automatically generated insights with severity levels:
   - **Green** - Healthy/positive finding
   - **Yellow** - Warning/attention needed
   - **Red** - Critical issue detected
4. **Recommendations** - Actionable suggestions based on findings
5. **ROI Estimates** - Business value calculations (when assumptions provided)

### Data quality scoring

Each report includes a confidence indicator based on:
- Parse success rate (how many rows had valid timestamps)
- Data completeness (coverage of required fields)
- Suspicious patterns (unusual null rates, gaps in data)

Confidence levels:
- **High** (green) - Data quality score 80+
- **Medium** (yellow) - Data quality score 50-79
- **Low** (red) - Data quality score below 50

### Progress tracking (API mode)

During API fetches, the progress panel shows:
- **Facility pills** - Color-coded status (queued, running, done, error) with page counts
- **ETA** - Estimated time remaining based on current throughput
- **Pages/second** - Real-time fetch rate
- **Report progress bars** - Visual percentage complete

---

## Advanced settings

Access via the gear icon to configure:

### Data handling

**Partial Period Handling** - Controls how incomplete time periods (e.g., a partial week at the start/end of your date range) appear in charts:
- **Include all data** - Show everything, including partial periods
- **Trim partial periods** - Remove incomplete first/last periods from charts
- **Highlight partial periods** - Show all data but visually distinguish partial periods

**Drill-Down** - When enabled (default), clicking on chart elements (bars, pie segments, or outlier points) opens a modal showing the underlying records. Available on select high-value charts:
- **Current Inventory** - Yard age buckets show trailer #, SCAC, age, and location
- **Trailer History** - Top carriers by lost events show trailer #, event date, and event type
- **Detention History** - Detention by carrier shows trailer #, detention hours, and date
- **Dock Door History** - Outlier points show trailer #, dwell time, and check-in/out times

Charts with drill-down capability display a "Drill-down" badge. The drill-down modal includes sortable columns and an "Export CSV" button to export just that subset of data.

### Web Worker

**Processing Mode** - Enable background processing for large datasets to keep the UI responsive. Recommended for most use cases. The worker automatically handles:
- Chunked row processing
- Progress updates without blocking the UI
- Automatic fallback to main thread if worker fails

### API Backpressure Overrides

Fine-tune API request behavior for advanced troubleshooting:

| Setting | Default | Description |
|---------|---------|-------------|
| Global Max Concurrency | 8 | Maximum simultaneous API requests |
| Green Zone Boost | 12 | Concurrency when conditions are optimal |
| Fetch Buffer Size | 9 | Pages fetched ahead of processing |
| Processing Pool Size | 3 | Pages processed simultaneously |
| Page Queue Limit | 10 | Memory safety valve |
| Force Tier | Auto | Override automatic dataset size detection |

---

## Exports

### Per-chart exports
- **PNG** - Download any chart as an image
- **CSV** - Download the data behind any chart (includes metadata header)

### Per-report exports
- **CSV** - Download aggregated metrics for each report

### Full assessment exports
- **Summary TXT** - Text summary of all findings, metrics, and recommendations
- **Print to PDF** - Browser print dialog with print-optimized styling

---

## Security & privacy

- **API tokens are never persisted** - Stored in memory only, cleared on page close or reset
- **No cookies or local storage** for sensitive data
- **PII protection** - Driver phone/cell values are automatically scrubbed and never displayed or exported
- **Static hosting** - No server-side processing; all analysis happens in your browser

---

## Mock mode

Enable "Mock mode" in the header to demo the tool without connecting to a live API or uploading files.

**Features:**
- Sample datasets for each report type
- Multiple facilities with different data patterns
- Simulates pagination for realistic testing

**Large dataset testing:** Add `?largedata=N` to the URL to generate N pages of synthetic data for performance testing.

---

## Troubleshooting

### Common issues

**"Parse failures" warning**
Some rows couldn't be processed, usually due to unexpected timestamp formats. Check the data quality score for impact.

**"Missing critical columns" (CSV mode)**
The uploaded file is missing required columns for the selected report type. Verify you exported the correct report from YMS.

**Charts not rendering**
Ensure JavaScript is enabled and try a different browser. Web Worker issues can be resolved by disabling the worker in Advanced Settings.

**API errors (401/403)**
Token may be expired or invalid. Re-paste the token and try again.

**API errors (429)**
Rate limited by the server. The tool will automatically retry with backoff. If persistent, reduce concurrency in Advanced Settings.

**Slow performance**
- For large datasets, ensure Web Worker is enabled
- Consider using CSV mode instead of API for very large date ranges
- Reduce the number of facilities or date range

**"Large dataset detected" message**
The tool automatically switches to conservative mode to prevent timeouts. This is normal for datasets with 200+ pages.

### Warnings panel

Click "Warnings" to expand a collapsible list of all warnings encountered during processing. Warnings include:
- CSV validation messages
- API retry notifications
- Data quality issues
- Processing anomalies

---

## Keyboard shortcuts

| Key | Action |
|-----|--------|
| Escape | Close Advanced Settings drawer or drill-down modal |

---

## URL parameters

| Parameter | Description |
|-----------|-------------|
| `?perf=1` | Enable performance instrumentation panel (no PII logged) |
| `?largedata=N` | Generate N pages of mock data for load testing |

---

## Technical details

- **Hosting:** GitHub Pages (static files only)
- **Stack:** Plain HTML/CSS/JS (ES Modules), Chart.js + Luxon via CDN
- **Browser requirements:** Modern browser with JavaScript enabled

### Project structure

```
index.html      # Layout + CDN scripts + module entry
styles.css      # YMS-branded UI + print styles
app.js          # UI controller/state + orchestration
api.js          # Fetching, pagination, retry, concurrency
analysis.js     # Streaming aggregations + findings + ROI
charts.js       # Chart.js rendering + PNG export
export.js       # Summary TXT, CSV export, print helpers
csv-parser.js   # CSV field mapping + validation
csv-import.js   # CSV upload UI + processing pipeline
mock-data.js    # Sample payloads for Mock mode
worker.js       # Web Worker for background processing
```

### Performance architecture

- **Streaming aggregation** - Discards raw rows after processing to minimize memory
- **Adaptive concurrency** - Starts conservative, ramps up when healthy, backs off on pressure
- **Throttled UI updates** - Progress renders ~5x/second to prevent main-thread thrashing
- **P² streaming quantiles** - Calculates median/p90 without storing full arrays
- **Memory pressure monitoring** - Automatically reduces concurrency when heap usage is high

---

## For developers

See [AGENTS.md](AGENTS.md) for AI navigation and code invariants.

### Extending the app

**Add a new report:**
1. Add report name to the UI list in app.js
2. Add field mappings in csv-parser.js
3. Add a streaming aggregator in analysis.js
4. Add chart definitions + dataset builders in charts.js
5. Add findings/recommendations + confidence rules in analysis.js
6. Ensure scrubber is applied to all rows before analysis/export

**Adjust concurrency/retries:**
Update defaults in api.js - see comments for `CONCURRENCY_*`, `RETRY_*`, and timeout constants.

**Add new export formats:**
Implement in export.js and wire buttons in app.js.
