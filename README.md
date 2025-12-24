# YMS QBR Assistant

A web-based tool for analyzing Yard Management Solutions (YMS) data to help Customer Experience and Sales teams quantify customer adoption, operational performance, and return on investment (ROI).

## What does this tool do?

The QBR (Quarterly Business Review) Assistant helps you:

1. **Analyze yard operations** - Understand how efficiently trailers, drivers, and dock doors are being utilized
2. **Identify operational issues** - Surface problems like lost trailers, detention events, and data quality gaps
3. **Calculate ROI estimates** - Quantify the business value of YMS adoption using customer-provided assumptions
4. **Generate reports** - Export findings as charts, CSVs, and printable PDFs for customer presentations

## Getting started

### Data input options

The tool supports two ways to load data:

**CSV Mode (default)** - Upload CSV files exported from the YMS system. Simply drag and drop files or click to browse. The tool auto-detects which report each file contains.

**API Mode** - Connect directly to the YMS API using:
- Client subdomain (e.g., "acme" for acme.api.ymshub.com)
- API token (kept in memory only, never saved)
- Date range for the query

### Required inputs

- **Facility codes** - One or more facility identifiers (one per line)
- **Timezone** - Used for grouping data by day/week/month and displaying timestamps
- **Reports to analyze** - Select one or more from the available report types

### ROI assumptions (optional)

To enable ROI calculations, provide any of these optional values:

| Assumption | Used for |
|------------|----------|
| Detention cost per hour | Calculating detention-related costs and savings |
| Labor fully loaded rate per hour | Estimating driver labor efficiency and costs |
| Target moves per driver per day | Benchmarking driver performance (default: 50) |
| Target turns per dock door per day | Benchmarking dock door throughput |
| Cost per dock door hour | Calculating dock door idle time costs |

**♻️ ROI button** - After running an assessment, you can adjust any ROI assumptions and click the refresh button to recalculate ROI values without re-fetching data. This allows quick "what-if" scenarios with different assumptions.

---

## Available reports

### Current Inventory
Snapshot of trailers currently in the yard, including equipment types, dwell times, and aging analysis.

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
   - 🟢 **Green** - Healthy/positive finding
   - 🟡 **Yellow** - Warning/attention needed
   - 🔴 **Red** - Critical issue detected
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

---

## Advanced settings

Access via the gear icon to configure:

### Data handling

**Partial Period Handling** - Controls how incomplete time periods (e.g., a partial week at the start/end of your date range) appear in charts:
- **Include all data** - Show everything, including partial periods
- **Trim partial periods** - Remove incomplete first/last periods from charts
- **Highlight partial periods** - Show all data but visually distinguish partial periods

### Web Worker

**Processing Mode** - Enable background processing for large datasets to keep the UI responsive. Recommended for most use cases.

### API Backpressure Overrides

Fine-tune API request behavior for advanced troubleshooting:
- Global concurrency limits
- Per-report lane caps
- Timeout settings
- Green zone (automatic performance boost) settings

---

## Exports

### Per-chart exports
- **PNG** - Download any chart as an image
- **CSV** - Download the data behind any chart

### Per-report exports
- **CSV** - Download aggregated metrics for each report

### Full assessment exports
- **Summary TXT** - Text summary of all findings and metrics
- **Print to PDF** - Browser print dialog with print-optimized styling

---

## Security & privacy

- **API tokens are never persisted** - Stored in memory only, cleared on page close or reset
- **No cookies or local storage** for sensitive data
- **PII protection** - Driver phone/cell values are automatically scrubbed and never displayed or exported
- **Static hosting** - No server-side processing; all analysis happens in your browser

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
mock-data.js    # Sample payloads for Mock mode
worker.js       # Web Worker for background processing
```

---

## Mock mode

Enable "Mock mode" in the header to demo the tool without connecting to a live API. Uses small embedded sample datasets and runs through the full analysis pipeline.

---

## Troubleshooting

### Common issues

**"Parse failures" warning** - Some rows couldn't be processed, usually due to unexpected timestamp formats. Check the data quality score for impact.

**Charts not rendering** - Ensure JavaScript is enabled and try a different browser. Web Worker issues can be resolved by disabling the worker in Advanced Settings.

**API errors (401/403)** - Token may be expired or invalid. Re-paste the token and try again.

**Slow performance** - For very large datasets, ensure Web Worker is enabled. Consider reducing the date range or number of facilities.

---

## For developers

See [AGENTS.md](AGENTS.md) for AI navigation and code invariants.

### Extending the app

**Add a new report:**
1. Add report name to the UI list in app.js
2. Add a streaming aggregator in analysis.js
3. Add chart definitions + dataset builders in charts.js
4. Add findings/recommendations + confidence rules in analysis.js
5. Ensure scrubber is applied to all rows before analysis/export

**Adjust concurrency/retries:**
Update defaults in api.js - see comments for `CONCURRENCY_*`, `RETRY_*`, and timeout constants.

**Add new export formats:**
Implement in export.js and wire buttons in app.js.

### Performance notes

- Streaming aggregation discards raw rows after processing
- Adaptive concurrency starts at 8, ramps to 20 when healthy
- UI updates throttled to ~5/sec to prevent main-thread thrashing
- P² streaming quantile estimation for median/p90 without storing arrays
