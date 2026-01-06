/**
 * Streaming aggregations + findings/recommendations + data quality.
 * PII policy enforced here: any key containing "cell" or "phone" is dropped immediately.
 * Driver cell numbers are NEVER stored, rendered, or exported—only presence/absence is allowed via boolean flags.
 */

let DateTimeImpl = null;

function getDateTime() {
  if (DateTimeImpl) return DateTimeImpl;
  // Browser main thread uses global Luxon; workers may provide self.luxon.
  const fromWindow = typeof window !== 'undefined' && window.luxon?.DateTime;
  const fromSelf = typeof self !== 'undefined' && self.luxon?.DateTime;
  DateTimeImpl = fromWindow || fromSelf || null;
  if (!DateTimeImpl) {
    throw new Error('Luxon DateTime is not available in this context.');
  }
  return DateTimeImpl;
}

export function setDateTimeImplementation(dt) {
  DateTimeImpl = dt;
}

const PII_KEY_RE = /(cell|phone)/i;

function isNil(v) {
  return v === null || v === undefined || v === '';
}

function safeStr(v) {
  return (v === null || v === undefined) ? '' : String(v).trim();
}

function normalizeBoolish(v) {
  if (typeof v === 'boolean') return v;
  const s = safeStr(v).toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes' || s === 'y') return true;
  if (s === '0' || s === 'false' || s === 'no' || s === 'n') return false;
  return null;
}

function parseFastParts(str) {
  // YYYY-MM-DD HH:mm:ss or YYYY-MM-DD HH:mm
  let m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(str);
  if (m) {
    return {
      year: Number(m[1]),
      month: Number(m[2]),
      day: Number(m[3]),
      hour: Number(m[4]),
      minute: Number(m[5]),
      second: Number(m[6] || 0),
    };
  }

  // MM-DD-YYYY HH:mm(:ss)?
  m = /^(\d{2})-(\d{2})-(\d{4})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(str);
  if (m) {
    return {
      year: Number(m[3]),
      month: Number(m[1]),
      day: Number(m[2]),
      hour: Number(m[4]),
      minute: Number(m[5]),
      second: Number(m[6] || 0),
    };
  }

  // MM/DD/YYYY HH:mm(:ss)?
  m = /^(\d{2})\/(\d{2})\/(\d{4})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/.exec(str);
  if (m) {
    return {
      year: Number(m[3]),
      month: Number(m[1]),
      day: Number(m[2]),
      hour: Number(m[4]),
      minute: Number(m[5]),
      second: Number(m[6] || 0),
    };
  }

  return null;
}

/**
 * Robust timestamp parsing (Luxon). Most timestamps assumed UTC.
 * Supports:
 * - ISO
 * - YYYY-MM-DD HH:mm:ss
 * - MM-DD-YYYY HH:mm
 * - MM/DD/YYYY HH:mm
 * If "treatAsLocal" is true, we parse as if already in the chosen timezone (no UTC conversion).
 */
export function parseTimestamp(raw, { timezone, assumeUTC = true, treatAsLocal = false, onFail } = {}) {
  const DateTime = getDateTime();
  if (isNil(raw)) return null;
  const s = safeStr(raw);
  if (!s) return null;

  // If field is already local facility time, treat it as local in selected timezone
  const zone = treatAsLocal ? timezone : (assumeUTC ? 'utc' : timezone);

  // Fast path for common timestamp formats without multiple Luxon allocations
  const fastParts = parseFastParts(s);
  if (fastParts) {
    // If assuming UTC, build epoch quickly to avoid extra Luxon parsing overhead.
    if (!treatAsLocal && assumeUTC) {
      const ms = Date.UTC(fastParts.year, fastParts.month - 1, fastParts.day, fastParts.hour, fastParts.minute, fastParts.second);
      const dtFastUtc = DateTime.fromMillis(ms, { zone });
      if (dtFastUtc.isValid) return dtFastUtc;
    } else {
      const dtFast = DateTime.fromObject(fastParts, { zone });
      if (dtFast.isValid) return dtFast;
    }
  }

  // Try ISO
  let dt = DateTime.fromISO(s, { zone });
  if (dt.isValid) return dt;

  // Common formats
  const fmts = [
    'yyyy-MM-dd HH:mm:ss',
    'yyyy-MM-dd HH:mm',
    'MM-dd-yyyy HH:mm',
    'MM/dd/yyyy HH:mm',
    'MM-dd-yyyy HH:mm:ss',
    'MM/dd/yyyy HH:mm:ss',
  ];

  for (const fmt of fmts) {
    dt = DateTime.fromFormat(s, fmt, { zone });
    if (dt.isValid) return dt;
  }

  // Sometimes API sends seconds since epoch
  if (/^\d{10,13}$/.test(s)) {
    const n = Number(s);
    if (Number.isFinite(n)) {
      dt = s.length === 13
        ? DateTime.fromMillis(n, { zone: treatAsLocal ? timezone : 'utc' })
        : DateTime.fromSeconds(n, { zone: treatAsLocal ? timezone : 'utc' });
      if (dt.isValid) return dt;
    }
  }

  onFail?.(`Timestamp parse failed: "${s}"`);
  return null;
}

/**
 * Strict PII scrubber + “presence” extraction.
 * Returns:
 *  - row: PII-scrubbed shallow copy
 *  - flags: booleans capturing allowed info (presence only)
 */
export function normalizeRowStrict(rawRow, { report, timezone, onWarning } = {}) {
  if (!rawRow || typeof rawRow !== 'object') return null;

  const flags = {
    // Driver contact presence (phone/cell keys) — boolean only.
    driverContactPresent: false,
    anyPhoneFieldPresent: false,
    // Allow special-case "timezone_arrival_time" handling in analyzers
    hasTimezoneArrivalTime: !isNil(rawRow.timezone_arrival_time),
  };

  const clean = {};
  for (const k in rawRow) {
    if (!Object.prototype.hasOwnProperty.call(rawRow, k)) continue;
    const v = rawRow[k];
    if (PII_KEY_RE.test(k)) {
      // Presence only. Never store value.
      const present = !isNil(v);
      flags.anyPhoneFieldPresent = flags.anyPhoneFieldPresent || present;

      // “Driver-ish” phone/cell keys used only for inference, not display
      if (/driver/i.test(k)) flags.driverContactPresent = flags.driverContactPresent || present;
      continue;
    }
    clean[k] = v;
  }

  return { row: clean, flags, report, timezone };
}

// ---------- Stats helpers ----------
class CounterMap {
  constructor() { this.map = new Map(); }
  inc(key, by = 1) {
    const k = safeStr(key) || '(blank)';
    this.map.set(k, (this.map.get(k) || 0) + by);
  }
  top(n = 10) {
    return Array.from(this.map.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, n)
      .map(([k, v]) => ({ key: k, value: v }));
  }
  toObjectSorted() {
    return Array.from(this.map.entries()).sort((a, b) => b[1] - a[1])
      .reduce((acc, [k, v]) => (acc[k] = v, acc), {});
  }
}

/**
 * P² quantile estimator (streaming). Great for median/p90 without storing raw arrays.
 * Reference: Jain & Chlamtac (1985).
 */
class P2Quantile {
  constructor(p) {
    this.p = p;
    this.n = 0;
    this.q = [];  // marker heights
    this.np = []; // desired marker positions
    this.ni = []; // actual marker positions
    this.dn = []; // increments
    this.init = [];
  }

  add(x) {
    if (!Number.isFinite(x)) return;
    this.n++;

    if (this.n <= 5) {
      this.init.push(x);
      if (this.n === 5) {
        this.init.sort((a, b) => a - b);
        this.q = [...this.init];
        this.ni = [1, 2, 3, 4, 5];
        this.np = [1, 1 + 2*this.p, 1 + 4*this.p, 3 + 2*this.p, 5];
        this.dn = [0, this.p/2, this.p, (1+this.p)/2, 1];
      }
      return;
    }

    // Find k
    let k = 0;
    if (x < this.q[0]) { this.q[0] = x; k = 0; }
    else if (x < this.q[1]) k = 0;
    else if (x < this.q[2]) k = 1;
    else if (x < this.q[3]) k = 2;
    else if (x <= this.q[4]) k = 3;
    else { this.q[4] = x; k = 3; }

    // Increment positions
    for (let i = 0; i < 5; i++) this.ni[i] += (i > k ? 1 : 0);
    for (let i = 0; i < 5; i++) this.np[i] += this.dn[i];

    // Adjust heights of markers 2-4
    for (let i = 1; i <= 3; i++) {
      const d = this.np[i] - this.ni[i];
      if ((d >= 1 && this.ni[i+1] - this.ni[i] > 1) || (d <= -1 && this.ni[i-1] - this.ni[i] < -1)) {
        const ds = Math.sign(d);
        const qi = this.parabolic(i, ds);
        if (this.q[i-1] < qi && qi < this.q[i+1]) this.q[i] = qi;
        else this.q[i] = this.linear(i, ds);
        this.ni[i] += ds;
      }
    }
  }

  parabolic(i, d) {
    const q = this.q, n = this.ni;
    return q[i] + (d / (n[i+1] - n[i-1])) * (
      (n[i] - n[i-1] + d) * (q[i+1] - q[i]) / (n[i+1] - n[i]) +
      (n[i+1] - n[i] - d) * (q[i] - q[i-1]) / (n[i] - n[i-1])
    );
  }

  linear(i, d) {
    const q = this.q, n = this.ni;
    return q[i] + d * (q[i + d] - q[i]) / (n[i + d] - n[i]);
  }

  value() {
    if (this.n === 0) return null;
    if (this.n <= 5) {
      const arr = [...this.init].sort((a, b) => a - b);
      const idx = Math.floor((arr.length - 1) * this.p);
      return arr[idx] ?? null;
    }
    return this.q[2]; // middle marker estimates quantile p
  }
}

/**
 * Approx distinct counter via bitset + linear counting.
 * We avoid storing sets of driver IDs per period.
 */
class ApproxDistinct {
  constructor(bits = 2048) {
    this.bits = bits;
    this.arr = new Uint8Array(bits / 8);
  }
  add(key) {
    const s = safeStr(key);
    if (!s) return;
    const h = fnv1a32(s);
    const idx = h % this.bits;
    this.arr[idx >> 3] |= (1 << (idx & 7));
  }
  estimate() {
    const m = this.bits;
    let zeros = 0;
    for (let i = 0; i < m; i++) {
      const byte = this.arr[i >> 3];
      const bit = (byte >> (i & 7)) & 1;
      if (bit === 0) zeros++;
    }
    if (zeros === 0) return m; // saturated
    return Math.round(-m * Math.log(zeros / m));
  }
}

function fnv1a32(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h >>> 0;
}

function scoreToBadge(score) {
  if (score >= 80) return { label: 'High', color: 'green' };
  if (score >= 55) return { label: 'Medium', color: 'yellow' };
  return { label: 'Low', color: 'red' };
}

// ---------- Confidence tooltip and trend analysis helpers ----------

/**
 * Generate tooltip text explaining confidence score factors per report type.
 */
function generateTooltipText(report, factors) {
  const lines = [];

  // Score
  lines.push(`Score: ${factors.score ?? '?'}/100`);

  // Parse success rate
  const total = (factors.parseOk ?? 0) + (factors.parseFails ?? 0);
  if (total > 0) {
    const parseRate = Math.round((factors.parseOk / total) * 100);
    lines.push(`Parse success: ${parseRate}% (${factors.parseOk}/${total})`);
  }

  // Report-specific factors
  switch (report) {
    case 'current_inventory':
      if (factors.stale30Pct !== undefined) {
        lines.push(`Stale records (>30d): ${factors.stale30Pct}%`);
      }
      if (factors.placeholderRate !== undefined && factors.placeholderRate > 0) {
        lines.push(`Placeholder SCAC: ${factors.placeholderRate}%`);
      }
      break;

    case 'detention_history':
      if (factors.coveragePct !== undefined) {
        lines.push(`Event coverage: ${factors.coveragePct}%`);
      }
      break;

    case 'dockdoor_history':
      if (factors.dwellCoveragePct !== undefined) {
        lines.push(`Dwell coverage: ${factors.dwellCoveragePct}%`);
      }
      if (factors.processCoveragePct !== undefined) {
        lines.push(`Process coverage: ${factors.processCoveragePct}%`);
      }
      break;

    case 'driver_history':
      if (factors.compliancePct !== undefined) {
        lines.push(`Timing compliance: ${factors.compliancePct}%`);
      }
      break;

    case 'trailer_history':
      // Simple parse-based score, no extra factors
      break;
  }

  return lines.join('\n');
}

/**
 * Generate a specific confidence reason for a finding based on data quality factors.
 * This provides actionable context to help users understand why a finding has its confidence level.
 *
 * @param {string} confidence - 'high', 'medium', or 'low'
 * @param {object} factors - Data quality factors available at the time of finding generation
 * @returns {string} A specific explanation for the confidence level
 */
function generateConfidenceReason(confidence, factors = {}) {
  const {
    parseOk = 0,
    parseFails = 0,
    nullRate = null,           // Percentage of null values in key fields
    coveragePct = null,        // Data coverage percentage
    sampleSize = null,         // Number of data points
    trendDataPoints = null,    // Number of periods for trend analysis
    dwellCoveragePct = null,   // Dwell time coverage
    processCoveragePct = null, // Process time coverage
    compliancePct = null,      // Timing compliance rate
    stale30Pct = null,         // Stale record percentage
    placeholderRate = null,    // Placeholder SCAC rate
    isTrendBased = false,      // Whether this is a trend-based finding
    isRatioBased = false,      // Whether this is based on ratio analysis
    isThresholdBased = false,  // Whether this is based on threshold comparison
  } = factors;

  const total = parseOk + parseFails;
  const parseRate = total > 0 ? Math.round((parseOk / total) * 100) : 100;
  const parseFailRate = total > 0 ? Math.round((parseFails / total) * 100) : 0;

  // Build specific reasons based on available factors
  const reasons = [];

  if (confidence === 'high') {
    // High confidence - explain what makes it reliable
    if (parseRate >= 95 && total > 0) {
      reasons.push(`${parseRate}% of records parsed successfully`);
    }
    if (coveragePct !== null && coveragePct >= 90) {
      reasons.push(`${coveragePct}% data coverage`);
    }
    if (sampleSize !== null && sampleSize >= 100) {
      reasons.push(`based on ${sampleSize.toLocaleString()} data points`);
    }
    if (trendDataPoints !== null && trendDataPoints >= 4) {
      reasons.push(`trend analysis spans ${trendDataPoints} periods`);
    }
    if (isThresholdBased) {
      reasons.push('clear threshold exceeded');
    }
    if (reasons.length === 0) {
      reasons.push('data quality and completeness are strong');
    }
  } else if (confidence === 'medium') {
    // Medium confidence - explain what's limiting it
    if (parseFailRate >= 5 && parseFailRate < 20) {
      reasons.push(`${parseFailRate}% of records failed to parse, which may affect accuracy`);
    }
    if (nullRate !== null && nullRate >= 10 && nullRate < 30) {
      reasons.push(`${nullRate}% null values in key fields may skew results`);
    }
    if (coveragePct !== null && coveragePct >= 50 && coveragePct < 90) {
      reasons.push(`only ${coveragePct}% of records have complete data`);
    }
    if (sampleSize !== null && sampleSize >= 20 && sampleSize < 100) {
      reasons.push(`based on ${sampleSize} data points (moderate sample)`);
    }
    if (trendDataPoints !== null && trendDataPoints >= 2 && trendDataPoints < 4) {
      reasons.push(`trend based on only ${trendDataPoints} periods`);
    }
    if (dwellCoveragePct !== null && dwellCoveragePct < 80) {
      reasons.push(`dwell time coverage is ${dwellCoveragePct}%`);
    }
    if (processCoveragePct !== null && processCoveragePct < 80) {
      reasons.push(`process time coverage is ${processCoveragePct}%`);
    }
    if (compliancePct !== null && compliancePct < 70) {
      reasons.push(`timing compliance is ${compliancePct}%`);
    }
    if (stale30Pct !== null && stale30Pct >= 10 && stale30Pct < 25) {
      reasons.push(`${stale30Pct}% of records are stale (>30 days old)`);
    }
    if (placeholderRate !== null && placeholderRate >= 10) {
      reasons.push(`${placeholderRate}% placeholder values detected`);
    }
    if (isRatioBased) {
      reasons.push('ratio analysis may not reflect all edge cases');
    }
    if (isTrendBased && trendDataPoints === null) {
      reasons.push('trend data has limited historical depth');
    }
    if (reasons.length === 0) {
      reasons.push('some data gaps or quality issues present');
    }
  } else {
    // Low confidence - explain serious limitations
    if (parseFailRate >= 20) {
      reasons.push(`${parseFailRate}% of records failed to parse, significantly affecting reliability`);
    }
    if (nullRate !== null && nullRate >= 30) {
      reasons.push(`${nullRate}% null values indicate substantial data gaps`);
    }
    if (coveragePct !== null && coveragePct < 50) {
      reasons.push(`only ${coveragePct}% data coverage - results may not be representative`);
    }
    if (sampleSize !== null && sampleSize < 20) {
      reasons.push(`only ${sampleSize} data points - sample too small for reliable conclusions`);
    }
    if (stale30Pct !== null && stale30Pct >= 25) {
      reasons.push(`${stale30Pct}% of records are stale, affecting data freshness`);
    }
    if (reasons.length === 0) {
      reasons.push('significant data quality issues limit reliability');
    }
  }

  // Join reasons into a readable sentence
  const reasonText = reasons.join('; ');
  const prefix = confidence === 'high' ? 'High confidence: '
    : confidence === 'medium' ? 'Medium confidence: '
    : 'Low confidence: ';

  return prefix + reasonText + '.';
}

/**
 * Extract time-series data from various data structures.
 * Returns { labels: string[], values: number[] } sorted chronologically.
 */
function extractTimeSeries(data, options = {}) {
  const { valueExtractor = null } = options;

  if (!data) return null;

  let labels, values;

  // CounterMap (has .map property)
  if (data.map instanceof Map) {
    labels = Array.from(data.map.keys()).sort();
    values = labels.map(k => data.map.get(k) || 0);
  }
  // Plain Map (e.g., P2Quantile estimators)
  else if (data instanceof Map) {
    labels = Array.from(data.keys()).sort();
    if (valueExtractor) {
      values = labels.map(k => valueExtractor(data.get(k)));
    } else {
      values = labels.map(k => {
        const v = data.get(k);
        // Try common quantile accessor patterns
        if (v?.median?.value) return v.median.value();
        if (typeof v === 'number') return v;
        return null;
      });
    }
  }
  // Pre-computed series { labels: [], median: [], p90: [] }
  else if (Array.isArray(data.labels) && Array.isArray(data.median)) {
    labels = data.labels;
    values = data.median;
  }
  else {
    return null;
  }

  // Filter out invalid values
  const validPairs = labels
    .map((l, i) => ({ label: l, value: values[i] }))
    .filter(p => Number.isFinite(p.value));

  return {
    labels: validPairs.map(p => p.label),
    values: validPairs.map(p => p.value)
  };
}

/**
 * Compute trend analysis comparing two consecutive periods.
 * Supports adaptive granularity: monthly -> weekly -> daily.
 */
function computeTrendAnalysis(dataByGranularity, metricName, options = {}) {
  const {
    significantChangePct = 15,
    valueExtractor = null
  } = options;

  // dataByGranularity can be: { monthly, weekly, daily } or a single data source
  // Prefer finer granularity first (daily → weekly → monthly) to match chart display
  const granularities = [
    { key: 'daily', label: 'day-over-day', data: dataByGranularity.daily },
    { key: 'weekly', label: 'week-over-week', data: dataByGranularity.weekly },
    { key: 'monthly', label: 'month-over-month', data: dataByGranularity.monthly || dataByGranularity }
  ];

  for (const { key, label, data } of granularities) {
    if (!data) continue;

    const series = extractTimeSeries(data, { valueExtractor });
    if (!series || series.labels.length < 2) continue;

    // Compare last two periods
    const n = series.labels.length;
    const current = { label: series.labels[n - 1], value: series.values[n - 1] };
    const previous = { label: series.labels[n - 2], value: series.values[n - 2] };

    if (!Number.isFinite(previous.value) || previous.value === 0) continue;

    const changePct = ((current.value - previous.value) / Math.abs(previous.value)) * 100;
    const direction = changePct > 0 ? 'increased' : 'decreased';
    const absChange = Math.abs(changePct);

    return {
      current,
      previous,
      changePct: Math.round(changePct * 10) / 10,
      direction,
      isSignificant: absChange >= significantChangePct,
      metricName,
      granularity: key,
      granularityLabel: label
    };
  }

  return null;
}

/**
 * Format a trend analysis result into a finding object.
 * @param {object} trend - Trend analysis result from computeTrendAnalysis
 * @param {object} options - Formatting options
 * @param {object} options.dataQualityFactors - Factors for generating confidence reason
 */
function formatTrendFinding(trend, options = {}) {
  const {
    increaseLevel = 'yellow',
    decreaseLevel = 'green',
    unit = '',
    invertLevels = false,  // true when decrease is bad (e.g., coverage dropping)
    roundValues = true,
    dataQualityFactors = {}
  } = options;

  if (!trend) return null;

  const absChange = Math.abs(trend.changePct);
  const level = trend.direction === 'increased'
    ? (invertLevels ? decreaseLevel : increaseLevel)
    : (invertLevels ? increaseLevel : decreaseLevel);

  const formatVal = (v) => {
    if (!Number.isFinite(v)) return '?';
    const val = roundValues ? Math.round(v * 10) / 10 : v;
    return `${val}${unit}`;
  };

  const currentVal = formatVal(trend.current.value);
  const prevVal = formatVal(trend.previous.value);

  // Count data points in the trend
  const trendDataPoints = trend.current.count !== undefined ? 2 : null;

  const confidenceReason = generateConfidenceReason('medium', {
    ...dataQualityFactors,
    isTrendBased: true,
    trendDataPoints
  });

  return {
    level,
    text: `${trend.metricName} ${trend.direction} ${absChange}% (${prevVal} → ${currentVal}) ${trend.granularityLabel}.`,
    confidence: 'medium',
    confidenceReason
  };
}

function monthKey(dt, timezone) {
  return dt.setZone(timezone).toFormat('yyyy-LL');
}
function weekKey(dt, timezone) {
  const z = dt.setZone(timezone);
  return `${z.weekYear}-W${String(z.weekNumber).padStart(2, '0')}`;
}
function dayKey(dt, timezone) {
  return dt.setZone(timezone).toFormat('yyyy-LL-dd');
}

function maybeNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function scacIsPlaceholder(scac) {
  const s = safeStr(scac).toUpperCase();
  return !s || s === 'XXXX' || s === 'UNKNOWN' || s === 'UNKN';
}

// ---------- Report analyzers ----------
class BaseAnalyzer {
  constructor({ timezone, startDate, endDate, assumptions, onWarning }) {
    this.timezone = timezone;
    this.startDate = startDate;
    this.endDate = endDate;
    this.assumptions = assumptions;
    this.onWarning = onWarning;

    this.totalRows = 0;
    this.parseFails = 0;
    this.parseOk = 0;

    this.warnings = [];

    // Track date range from ingested data (for CSV mode)
    this.earliestDate = null; // DateTime object
    this.latestDate = null;   // DateTime object
  }

  /**
   * Track a date for date range inference.
   * Call this with any valid DateTime during ingest.
   */
  trackDate(dt) {
    if (!dt || !dt.isValid) return;
    if (!this.earliestDate || dt < this.earliestDate) {
      this.earliestDate = dt;
    }
    if (!this.latestDate || dt > this.latestDate) {
      this.latestDate = dt;
    }
  }

  /**
   * Get the inferred date range as ISO date strings.
   */
  getInferredDateRange() {
    return {
      startDate: this.earliestDate?.toISODate() || null,
      endDate: this.latestDate?.toISODate() || null,
      isInferred: true,
    };
  }

  warn(msg) {
    this.onWarning?.(msg);
    this.warnings.push(msg);
  }

  dataQualityScore() {
    const total = this.parseOk + this.parseFails;
    const parseRate = total > 0 ? (this.parseOk / total) : 1;
    // Base score biased by parse health; subclasses layer coverage into final.
    return Math.round(parseRate * 100);
  }
}

class CurrentInventoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);
    this.totalTrailers = 0;

    this.updatedBuckets = { '0–1d': 0, '1–7d': 0, '7–30d': 0, '30d+': 0, 'unknown': 0 };
    // CSV mode yard-age buckets (using Elapsed Time (Hours))
    this.yardAgeBuckets = { '0-1d': 0, '1-7d': 0, '7-30d': 0, '30d+': 0, 'unknown': 0 };
    this.hasCSVYardAge = false;

    this.moveType = new CounterMap();
    this.outbound = 0;
    this.inbound = 0;
    this.placeholderScac = 0;
    this.scacTotal = 0;

    this.liveLoads = 0;
    this.liveLoadMissingDriverContact = 0; // presence only
  }

  ingest({ row, flags }) {
    this.totalRows++;
    this.totalTrailers++;
    const DateTime = getDateTime();

    // move_type_name distribution
    const mt = safeStr(row.move_type_name);
    if (mt) this.moveType.inc(mt);
    const mtLower = mt.toLowerCase();
    if (mtLower.includes('out')) this.outbound++;
    if (mtLower.includes('in')) this.inbound++;

    // SCAC placeholder
    const scac = row.scac ?? row.carrier_scac ?? row.scac_code;
    if (!isNil(scac)) {
      this.scacTotal++;
      if (scacIsPlaceholder(scac)) this.placeholderScac++;
    }

    // updated_at recency buckets
    const dt = parseTimestamp(row.updated_at, {
      timezone: this.timezone,
      assumeUTC: true,
      treatAsLocal: false,
      onFail: () => { this.parseFails++; }
    });
    if (dt) {
      this.parseOk++;
      this.trackDate(dt); // Track for date range inference
      const now = DateTime.now().setZone(this.timezone);
      const ageHours = now.diff(dt.setZone(this.timezone), 'hours').hours;
      if (!Number.isFinite(ageHours)) this.updatedBuckets.unknown++;
      else if (ageHours <= 24) this.updatedBuckets['0–1d']++;
      else if (ageHours <= 24 * 7) this.updatedBuckets['1–7d']++;
      else if (ageHours <= 24 * 30) this.updatedBuckets['7–30d']++;
      else this.updatedBuckets['30d+']++;
    } else {
      this.updatedBuckets.unknown++;
    }

    // live load driver contact presence inference (never store number)
    const live = normalizeBoolish(row.live_load) ?? (row.live_load == 1);
    if (live) {
      this.liveLoads++;
      if (!flags.driverContactPresent) this.liveLoadMissingDriverContact++;
    }

    // CSV-specific: yard-age buckets from Elapsed Time (Hours)
    if (row.csv_elapsed_hours !== undefined && row.csv_elapsed_hours !== null && row.csv_elapsed_hours !== '') {
      this.hasCSVYardAge = true;
      const hours = parseFloat(row.csv_elapsed_hours);
      if (Number.isFinite(hours) && hours >= 0) {
        if (hours <= 24) this.yardAgeBuckets['0-1d']++;
        else if (hours <= 168) this.yardAgeBuckets['1-7d']++;
        else if (hours <= 720) this.yardAgeBuckets['7-30d']++;
        else this.yardAgeBuckets['30d+']++;
      } else {
        this.yardAgeBuckets['unknown']++;
      }
    }
  }

  buildCharts(moveTypeTop, updatedSeries) {
    const charts = [
      {
        id: 'move_type_distribution',
        title: 'Move type distribution',
        kind: 'pie',
        description: 'Distribution of move_type_name values in current inventory.',
        data: {
          labels: Object.keys(moveTypeTop),
          datasets: [{ label: 'Count', data: Object.values(moveTypeTop) }]
        },
        csv: {
          columns: ['move_type_name', 'count'],
          rows: Object.entries(moveTypeTop).map(([k, v]) => ({ move_type_name: k, count: v })),
        }
      },
    ];

    // Add updated_at recency chart for API mode only (not CSV)
    if (!this.hasCSVYardAge) {
      charts.push({
        id: 'updated_recency_buckets',
        title: 'Updated_at recency buckets',
        kind: 'bar',
        description: 'How recently inventory records were updated.',
        data: {
          labels: updatedSeries.map(r => r.bucket),
          datasets: [{ label: 'Records', data: updatedSeries.map(r => r.count) }]
        },
        csv: {
          columns: ['bucket', 'count'],
          rows: updatedSeries,
        }
      });
    }

    // Add yard-age chart for CSV mode only
    if (this.hasCSVYardAge) {
      const yardAgeSeries = Object.entries(this.yardAgeBuckets)
        .filter(([k]) => k !== 'unknown')
        .map(([bucket, count]) => ({ bucket, count }));

      charts.push({
        id: 'yard_age_distribution',
        title: 'Trailer yard-age distribution',
        kind: 'bar',
        description: 'How long trailers have been in the yard (from CSV Elapsed Time Hours).',
        data: {
          labels: yardAgeSeries.map(r => r.bucket),
          datasets: [{ label: 'Trailers', data: yardAgeSeries.map(r => r.count) }]
        },
        csv: {
          columns: ['yard_age_bucket', 'count'],
          rows: yardAgeSeries.map(r => ({ yard_age_bucket: r.bucket, count: r.count })),
        }
      });
    }

    return charts;
  }

  finalize(meta) {
    const trailers = this.totalTrailers;
    const pct = (n, d) => d ? Math.round((n / d) * 1000) / 10 : 0;

    const updatedTotal = Object.values(this.updatedBuckets).reduce((a, b) => a + b, 0);
    const pctUpdated = (bucketLabel) => pct(this.updatedBuckets[bucketLabel], updatedTotal);

    const placeholderRate = this.scacTotal ? pct(this.placeholderScac, this.scacTotal) : 0;

    const outboundInboundRatio = this.inbound ? (this.outbound / this.inbound) : null;

    const missingDriverRate = this.liveLoads ? pct(this.liveLoadMissingDriverContact, this.liveLoads) : null;

    const stale30 = pctUpdated('30d+');

    // Data quality findings (move to tooltip)
    const dataQualityFindings = [];
    if (stale30 >= 10) {
      dataQualityFindings.push({
        level: stale30 >= 25 ? 'red' : 'yellow',
        text: `${stale30}% of records older than 30 days.`
      });
    }
    if (placeholderRate >= 10) {
      dataQualityFindings.push({ level: 'yellow', text: `Placeholder SCAC rate: ${placeholderRate}%.` });
    }
    if (missingDriverRate !== null && missingDriverRate >= 30) {
      dataQualityFindings.push({ level: 'yellow', text: `Live loads missing driver contact: ${missingDriverRate}%.` });
    }

    // Business findings (keep in Findings section)
    const findings = [];
    const recs = [];

    // Data quality factors for confidence reasons
    const dqFactors = {
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      sampleSize: this.totalRows,
      stale30Pct: stale30,
      placeholderRate
    };

    // Inventory health summary (business insight, not data quality)
    if (stale30 < 10) {
      findings.push({
        level: 'green',
        text: 'Inventory recency looks healthy (low share older than 30 days).',
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', { ...dqFactors, isThresholdBased: true })
      });
    } else if (stale30 >= 25) {
      findings.push({
        level: 'red',
        text: `${stale30}% of inventory records are older than 30 days - may indicate stale or abandoned assets.`,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', { ...dqFactors, isThresholdBased: true })
      });
      recs.push('Review update workflows and integrations to ensure inventory stays current.');
    } else {
      findings.push({
        level: 'yellow',
        text: `${stale30}% of inventory records are older than 30 days.`,
        confidence: 'medium',
        confidenceReason: generateConfidenceReason('medium', { ...dqFactors, stale30Pct: stale30 })
      });
      recs.push('Spot-check stale records and confirm whether they represent inactive assets or missed updates.');
    }

    // Outbound/inbound ratio (business insight)
    if (outboundInboundRatio !== null) {
      if (outboundInboundRatio > 2) {
        findings.push({
          level: 'yellow',
          text: `Outbound-heavy inventory ratio (${outboundInboundRatio.toFixed(1)}:1 outbound to inbound).`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isRatioBased: true })
        });
        recs.push('Review if outbound staging is backing up or if inbound flow is constrained.');
      } else if (outboundInboundRatio < 0.5) {
        findings.push({
          level: 'yellow',
          text: `Inbound-heavy inventory ratio (${(1/outboundInboundRatio).toFixed(1)}:1 inbound to outbound).`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isRatioBased: true })
        });
      }
    }

    // SCAC recommendations
    if (placeholderRate >= 10) {
      recs.push('Enforce SCAC validation and/or integrate carrier master data to reduce UNKNOWN/XXXX records.');
    }

    // Driver contact recommendations
    if (missingDriverRate !== null && missingDriverRate >= 30) {
      recs.push('If texting is expected, confirm driver contact capture/permissions and train gate/dispatch to populate contact fields.');
    }

    const dq = Math.round(
      0.65 * this.dataQualityScore() +
      0.35 * (updatedTotal ? pct(this.parseOk, this.parseOk + this.parseFails) : 100)
    );
    const badge = scoreToBadge(dq);

    // Generate tooltip text for confidence badge
    const tooltipText = generateTooltipText('current_inventory', {
      score: dq,
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      stale30Pct: stale30,
      placeholderRate
    });

    // Charts (2+)
    const moveTypeTop = this.moveType.toObjectSorted();
    const updatedSeries = Object.entries(this.updatedBuckets).map(([bucket, count]) => ({ bucket, count }));

    return {
      report: 'current_inventory',
      meta,
      inferredDateRange: this.getInferredDateRange(),
      dataQuality: {
        score: dq,
        ...badge,
        parseOk: this.parseOk,
        parseFails: this.parseFails,
        totalRows: this.totalRows,
        tooltipText,
        dataQualityFindings
      },
      metrics: {
        total_trailers: trailers,
        updated_last_24h_pct: pctUpdated('0–1d'),
        updated_last_7d_pct: Math.round(((this.updatedBuckets['0–1d'] + this.updatedBuckets['1–7d']) / (updatedTotal || 1)) * 1000) / 10,
        updated_last_30d_pct: Math.round(((this.updatedBuckets['0–1d'] + this.updatedBuckets['1–7d'] + this.updatedBuckets['7–30d']) / (updatedTotal || 1)) * 1000) / 10,
        placeholder_scac_pct: placeholderRate,
        outbound_vs_inbound_ratio: outboundInboundRatio,
        live_load_missing_driver_contact_pct: missingDriverRate,
      },
      charts: this.buildCharts(moveTypeTop, updatedSeries),
      findings,
      recommendations: recs,
      roi: null, // current inventory doesn’t drive ROI directly in MVP
      exports: {
        reportSummaryCsv: null, // built in export.js on demand
      }
    };
  }
}

class DetentionHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);
    this.preDetention = 0;
    this.detention = 0;
    this.prevented = 0;

    // Track live/drop for ALL rows (for general coverage)
    this.live = 0;
    this.drop = 0;

    // Track live/drop ONLY for detention events (for the pie chart)
    this.detentionLive = 0;
    this.detentionDrop = 0;

    // Multi-granularity time series for trend analysis
    this.monthlyDetention = new CounterMap();
    this.weeklyDetention = new CounterMap();
    this.dailyDetention = new CounterMap();
    this.monthlyPrevented = new CounterMap();
    this.weeklyPrevented = new CounterMap();
    this.dailyPrevented = new CounterMap();

    // Track SCAC ONLY for detention events (for the carrier bar chart)
    this.detentionByScac = new CounterMap();

    // Detention spend tracking
    this.detentionEventsWithDeparture = 0;
    this.totalDetentionHours = 0;

    // Track data source for CSV mode warning
    this.isCSVMode = opts.isCSVMode || false;
  }

  ingest({ row }) {
    this.totalRows++;

    const pre = parseTimestamp(row.pre_detention_start_time, {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    const det = parseTimestamp(row.detention_start_time, {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });

    // For trend grouping, pick a best event time
    const eventDt = det || pre;

    if (pre) { this.preDetention++; this.parseOk++; this.trackDate(pre); }
    if (det) { this.detention++; this.parseOk++; this.trackDate(det); }
    if (pre && !det) this.prevented++;

    // Track live/drop for ALL rows (general coverage metrics)
    const live = normalizeBoolish(row.live_load) ?? (row.live_load == 1);
    if (live === true) this.live++;
    else if (live === false) this.drop++;

    // Track live/drop and SCAC ONLY for detention events (for charts)
    // A detention event is when det is not null (detention_start_time exists)
    // OR for CSV mode, also consider pre-detention (pre_detention_start_time exists)
    const isDetentionEvent = det || pre;
    if (isDetentionEvent) {
      if (live === true) this.detentionLive++;
      else if (live === false) this.detentionDrop++;

      const scac = row.scac ?? row.carrier_scac ?? row.scac_code;
      if (!isNil(scac)) this.detentionByScac.inc(scac);
    }

    if (eventDt) {
      const mk = monthKey(eventDt, this.timezone);
      const wk = weekKey(eventDt, this.timezone);
      const dk = dayKey(eventDt, this.timezone);
      if (det) {
        this.monthlyDetention.inc(mk);
        this.weeklyDetention.inc(wk);
        this.dailyDetention.inc(dk);
      }
      if (pre && !det) {
        this.monthlyPrevented.inc(mk);
        this.weeklyPrevented.inc(wk);
        this.dailyPrevented.inc(dk);
      }
    }

    // Track detention spend: detention time to departure time
    // After CSV normalization: detention_start_time and departure_datetime are combined timestamps
    // API mode: same fields exist directly

    // Use already-parsed `det` for detention start (from detention_start_time)
    if (det) {
      // Try to find departure datetime
      // CSV normalization creates 'departure_datetime' from "Departure Date" + "Departure Time"
      // API may have the same or similar fields
      const depRaw = firstPresent(row, [
        'departure_datetime',  // CSV normalized field
        'depart_datetime', 'yard_out_time', 'left_yard_time',
        'checkout_time', 'actual_departure_time'
      ]);

      let depDt = null;
      if (depRaw) {
        // CSV departure_datetime is in local time format like "12-04-2025 11:39"
        depDt = parseTimestamp(depRaw, { timezone: this.timezone, treatAsLocal: true });
      }

      // Fallback: try separate date + time columns (in case normalization didn't happen)
      if (!depDt) {
        const depDate = firstPresent(row, ['csv_departure_date', 'departure_date', 'depart_date', 'out_date']);
        const depTime = firstPresent(row, ['csv_departure_time', 'departure_time', 'depart_time', 'out_time']);
        if (!isNil(depDate) && !isNil(depTime)) {
          depDt = parseTimestamp(`${depDate} ${depTime}`, { timezone: this.timezone, treatAsLocal: true });
        }
      }

      if (depDt && depDt > det) {
        const hours = depDt.diff(det, 'hours').hours;
        if (Number.isFinite(hours) && hours > 0) {
          this.detentionEventsWithDeparture++;
          this.totalDetentionHours += hours;
        }
      }
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    // Data quality findings (move to tooltip)
    const dataQualityFindings = [];
    if (this.detention === 0 && this.preDetention === 0) {
      // Add to main findings (not just tooltip) so user clearly sees the issue
      findings.push({
        level: 'yellow',
        text: 'No detention events found in this data. Detention rules may not be configured in YMS.',
        confidence: 'high',
        confidenceReason: 'No detention or pre-detention signals detected in the uploaded data.'
      });
      recs.push('Verify detention rules are configured in YMS. If detention tracking is not needed, this finding can be ignored.');
    }

    // CSV mode warning about prevented detention data
    if (this.isCSVMode && this.prevented === 0 && this.preDetention > 0) {
      findings.push({
        level: 'yellow',
        text: 'CSV data does not capture "Prevented Detention" events. Use API mode or review the Detention Dashboard in YMS for prevention metrics.',
        confidence: 'high',
        confidenceReason: 'CSV exports typically only include detention events that occurred, not events that were prevented by early departure.'
      });
    }

    // Calculate coverage for dqFactors
    const coverage = (this.totalRows ? Math.min(100, Math.round(((this.preDetention + this.detention) / this.totalRows) * 100)) : 100);

    // Data quality factors for confidence reasons
    const dqFactors = {
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      sampleSize: this.totalRows,
      coveragePct: coverage
    };

    // Trend analysis: Detention events
    const detentionTrend = computeTrendAnalysis(
      { monthly: this.monthlyDetention, weekly: this.weeklyDetention, daily: this.dailyDetention },
      'Detention events',
      { significantChangePct: 20 }
    );
    if (detentionTrend) {
      if (detentionTrend.isSignificant) {
        const finding = formatTrendFinding(detentionTrend, {
          increaseLevel: 'yellow',  // More detention is concerning
          decreaseLevel: 'green',
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);
        if (detentionTrend.direction === 'increased') {
          recs.push('Detention trending up - investigate carrier performance and dock scheduling.');
        }
      } else {
        findings.push({
          level: 'green',
          text: `Detention events stable at ~${Math.round(detentionTrend.current.value)} ${detentionTrend.granularityLabel}.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isTrendBased: true })
        });
      }
    }

    // Trend analysis: Prevented detention
    const preventedTrend = computeTrendAnalysis(
      { monthly: this.monthlyPrevented, weekly: this.weeklyPrevented, daily: this.dailyPrevented },
      'Prevented detention',
      { significantChangePct: 20 }
    );
    if (preventedTrend?.isSignificant) {
      const finding = formatTrendFinding(preventedTrend, {
        increaseLevel: 'green',   // More prevented is good
        decreaseLevel: 'yellow',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // Summary finding when no trends available but data exists
    if (!detentionTrend && !preventedTrend && (this.detention > 0 || this.preDetention > 0)) {
      findings.push({
        level: 'green',
        text: `Detention: ${this.detention} events, Prevented: ${this.prevented}.`,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: this.detention + this.preDetention })
      });
    }

    // Live/drop split (business finding)
    const liveDrop = (this.live + this.drop) ? Math.round((this.live / (this.live + this.drop)) * 1000) / 10 : null;
    if (liveDrop !== null && liveDrop > 80) {
      findings.push({
        level: 'yellow',
        text: `Detention heavily live-load skewed (~${liveDrop}% live).`,
        confidence: 'medium',
        confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isRatioBased: true })
      });
      recs.push('If drops are common, confirm drop workflow timestamps are being captured.');
    }

    // Recommendations for all-Live or all-Drop detention events
    const totalDetentionEvents = this.detentionLive + this.detentionDrop;
    if (totalDetentionEvents > 0) {
      if (this.detentionLive > 0 && this.detentionDrop === 0) {
        recs.push('All detention events are for Live loads. Consider adding detention tracking for Drop loads.');
      } else if (this.detentionDrop > 0 && this.detentionLive === 0) {
        recs.push('All detention events are for Drop loads. Consider adding detention tracking for Live loads.');
      }
    }

    const dqBase = this.dataQualityScore();
    const dq = Math.round(0.6 * dqBase + 0.4 * coverage);
    const badge = scoreToBadge(dq);

    // Generate tooltip text for confidence badge
    const tooltipText = generateTooltipText('detention_history', {
      score: dq,
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      coveragePct: coverage
    });

    // Charts - pick best granularity dynamically
    const detentionGranularity = pickBestGranularity({
      monthly: this.monthlyDetention,
      weekly: this.weeklyDetention,
      daily: this.dailyDetention
    });
    const preventedGranularity = pickBestGranularity({
      monthly: this.monthlyPrevented,
      weekly: this.weeklyPrevented,
      daily: this.dailyPrevented
    });

    // Use the finer of the two granularities for the combined chart
    // This ensures short data ranges (e.g., 1 week) use daily/weekly instead of falling back to monthly
    const granularityOrder = { month: 0, week: 1, day: 2 };
    const useGranularity = granularityOrder[detentionGranularity.granularity] >= granularityOrder[preventedGranularity.granularity]
      ? detentionGranularity : preventedGranularity;

    const detentionData = useGranularity.granularity === 'day' ? this.dailyDetention
      : useGranularity.granularity === 'week' ? this.weeklyDetention : this.monthlyDetention;
    const preventedData = useGranularity.granularity === 'day' ? this.dailyPrevented
      : useGranularity.granularity === 'week' ? this.weeklyPrevented : this.monthlyPrevented;

    const seriesMerged = mergeMonthSeries(detentionData, preventedData);
    const timeLabel = useGranularity.granularity;
    const chartTitle = `Detention vs prevented detention (${useGranularity.label})`;

    return {
      report: 'detention_history',
      meta,
      inferredDateRange: this.getInferredDateRange(),
      dataQuality: {
        score: dq,
        ...badge,
        parseOk: this.parseOk,
        parseFails: this.parseFails,
        totalRows: this.totalRows,
        coveragePct: coverage,
        tooltipText,
        dataQualityFindings
      },
      metrics: {
        pre_detention_count: this.preDetention,
        detention_count: this.detention,
        prevented_detention_count: this.prevented,
        live_load_count: this.detentionLive,
        drop_load_count: this.detentionDrop,
      },
      charts: [
        {
          id: `detention_vs_prevented_${useGranularity.label}`,
          title: chartTitle,
          kind: 'line',
          description: `${useGranularity.label.charAt(0).toUpperCase() + useGranularity.label.slice(1)} counts (timezone-adjusted grouping: ${meta.timezone}).`,
          data: {
            labels: seriesMerged.labels,
            datasets: [
              { label: 'Detention', data: seriesMerged.detention },
              { label: 'Prevented detention', data: seriesMerged.prevented },
            ]
          },
          csv: {
            columns: [timeLabel, 'detention_count', 'prevented_detention_count', 'timezone'],
            rows: seriesMerged.labels.map((m, i) => ({
              [timeLabel]: m,
              detention_count: seriesMerged.detention[i],
              prevented_detention_count: seriesMerged.prevented[i],
              timezone: meta.timezone
            })),
          }
        },
        // Detention events by Live/Drop (pie chart)
        ...(this.detentionLive + this.detentionDrop > 0 ? [{
          id: 'detention_live_drop',
          title: 'Detention events by load type',
          kind: 'pie',
          description: `Of ${this.detentionLive + this.detentionDrop} detention events, how many were live vs drop loads.`,
          data: {
            labels: ['Live load', 'Drop load'],
            datasets: [{
              label: 'Detention events',
              data: [this.detentionLive, this.detentionDrop]
            }]
          },
          csv: {
            columns: ['load_type', 'detention_events'],
            rows: [
              { load_type: 'Live load', detention_events: this.detentionLive },
              { load_type: 'Drop load', detention_events: this.detentionDrop },
            ]
          }
        }] : []),
        // Detention events by carrier (bar chart)
        ...(this.detentionByScac.map.size > 0 ? [{
          id: 'detention_by_carrier',
          title: 'Detention events by carrier',
          kind: 'bar',
          description: 'Top carriers with the most detention events.',
          data: {
            labels: this.detentionByScac.top(10).map(x => x.key),
            datasets: [{
              label: 'Detention events',
              data: this.detentionByScac.top(10).map(x => x.value)
            }]
          },
          csv: {
            columns: ['carrier_scac', 'detention_events'],
            rows: this.detentionByScac.top(10).map(x => ({ carrier_scac: x.key, detention_events: x.value }))
          }
        }] : [])
      ],
      findings,
      recommendations: recs,
      roi: computeDetentionROIIfEnabled({ meta, metrics: { prevented: this.prevented }, assumptions: meta.assumptions }),
      // Additional ROI for detention spend
      detentionSpend: computeDetentionSpendIfEnabled({
        metrics: {
          detentionEvents: this.detentionEventsWithDeparture,
          totalDetentionHours: this.totalDetentionHours,
          actualDetentionCount: this.detention,  // True count of detention events (for PM note)
        },
        assumptions: meta.assumptions,
      }),
    };
  }
}

class DockDoorHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);

    this.dwellCoverage = { ok: 0, total: 0 };
    this.processCoverage = { ok: 0, total: 0 };

    // Multi-granularity time series for trend analysis (monthly -> weekly -> daily fallback)
    this.dwellByMonth = new Map();   // key -> { median, p90 }
    this.dwellByWeek = new Map();
    this.dwellByDay = new Map();
    this.processByMonth = new Map();
    this.processByWeek = new Map();
    this.processByDay = new Map();

    this.processedBy = new CounterMap();
    this.moveRequestedBy = new CounterMap();
    this.rowsWithRequester = 0;

    // Dock door throughput tracking
    this.turnsByDoorByDay = new Map(); // key: day -> Map<door, count>
    this.uniqueDoors = new Set();
    this.totalTurns = 0;
    this.daysWithData = new Set();

    // Multi-granularity throughput tracking for time series chart
    this.turnsByDay = new CounterMap();   // day -> total turns
    this.turnsByWeek = new CounterMap();  // week -> total turns
    this.turnsByMonth = new CounterMap(); // month -> total turns
    this.doorsByDay = new Map();   // day -> Set<door>
    this.doorsByWeek = new Map();  // week -> Set<door>
    this.doorsByMonth = new Map(); // month -> Set<door>
  }

  getEstimators(map, key) {
    if (!map.has(key)) {
      map.set(key, { median: new P2Quantile(0.5), p90: new P2Quantile(0.9) });
    }
    return map.get(key);
  }

  ingest({ row }) {
    this.totalRows++;

    // Dwell start/end candidates
    const dwellStartRaw = firstPresent(row, ['dwell_start_time', 'dwell_start', 'dwell_in_time', 'dwell_start_time_utc']);
    const dwellEndRaw = firstPresent(row, ['dwell_end_time', 'dwell_end', 'dwell_out_time', 'dwell_end_time_utc']);

    // Process start/end candidates
    const procStartRaw = firstPresent(row, ['process_start_time', 'process_start', 'process_start_time_utc']);
    const procEndRaw = firstPresent(row, ['process_end_time', 'process_end', 'process_end_time_utc']);

    const dwellStart = parseTimestamp(dwellStartRaw, { timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; } });
    const dwellEnd = parseTimestamp(dwellEndRaw, { timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; } });

    const procStart = parseTimestamp(procStartRaw, { timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; } });
    const procEnd = parseTimestamp(procEndRaw, { timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; } });

    if (dwellStart && dwellEnd) {
      this.dwellCoverage.ok++; this.parseOk++;
      this.trackDate(dwellStart); // Track for date range inference
      this.trackDate(dwellEnd);
      const mins = dwellEnd.diff(dwellStart, 'minutes').minutes;
      if (Number.isFinite(mins) && mins >= 0) {
        // Track at multiple granularities for trend analysis fallback
        const mk = monthKey(dwellStart, this.timezone);
        const wk = weekKey(dwellStart, this.timezone);
        const dk = dayKey(dwellStart, this.timezone);
        this.getEstimators(this.dwellByMonth, mk).median.add(mins);
        this.getEstimators(this.dwellByWeek, wk).median.add(mins);
        this.getEstimators(this.dwellByDay, dk).median.add(mins);
      }
    }
    this.dwellCoverage.total++;

    if (procStart && procEnd) {
      this.processCoverage.ok++; this.parseOk++;
      const mins = procEnd.diff(procStart, 'minutes').minutes;
      if (Number.isFinite(mins) && mins >= 0) {
        // Track at multiple granularities for trend analysis fallback
        const mk = monthKey(procStart, this.timezone);
        const wk = weekKey(procStart, this.timezone);
        const dk = dayKey(procStart, this.timezone);
        this.getEstimators(this.processByMonth, mk).median.add(mins);
        this.getEstimators(this.processByWeek, wk).median.add(mins);
        this.getEstimators(this.processByDay, dk).median.add(mins);
      }
    }
    this.processCoverage.total++;

    // Leaderboards (only if sample size sufficient)
    const processedBy = safeStr(firstPresent(row, ['processed_by', 'processed_by_name', 'processed_by_user']));
    if (processedBy) this.processedBy.inc(processedBy);

    const requestedBy = safeStr(firstPresent(row, ['move_requested_by', 'requested_by', 'move_requested_by_name']));
    if (requestedBy) {
      this.moveRequestedBy.inc(requestedBy);
      this.rowsWithRequester++;
    }

    // Track dock door turns for throughput ROI
    const door = safeStr(firstPresent(row, ['door', 'door_name', 'dock_door', 'dock_door_name', 'door_id', 'location', 'location_name']));
    const eventDt = dwellStart || procStart;
    if (door && eventDt) {
      const dk = dayKey(eventDt, this.timezone);
      const wk = weekKey(eventDt, this.timezone);
      const mk = monthKey(eventDt, this.timezone);

      this.uniqueDoors.add(door);
      this.daysWithData.add(dk);
      this.totalTurns++;

      if (!this.turnsByDoorByDay.has(dk)) {
        this.turnsByDoorByDay.set(dk, new Map());
      }
      const dayMap = this.turnsByDoorByDay.get(dk);
      dayMap.set(door, (dayMap.get(door) || 0) + 1);

      // Track turns at all granularities for time series chart
      this.turnsByDay.inc(dk);
      this.turnsByWeek.inc(wk);
      this.turnsByMonth.inc(mk);

      // Track unique doors per period for utilization chart
      if (!this.doorsByDay.has(dk)) this.doorsByDay.set(dk, new Set());
      this.doorsByDay.get(dk).add(door);
      if (!this.doorsByWeek.has(wk)) this.doorsByWeek.set(wk, new Set());
      this.doorsByWeek.get(wk).add(door);
      if (!this.doorsByMonth.has(mk)) this.doorsByMonth.set(mk, new Set());
      this.doorsByMonth.get(mk).add(door);
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    const dwellCoveragePct = this.dwellCoverage.total ? Math.round((this.dwellCoverage.ok / this.dwellCoverage.total) * 1000) / 10 : 0;
    const processCoveragePct = this.processCoverage.total ? Math.round((this.processCoverage.ok / this.processCoverage.total) * 1000) / 10 : 0;

    // Data quality factors for confidence reasons
    const dqFactors = {
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      sampleSize: this.totalRows,
      dwellCoveragePct,
      processCoveragePct
    };

    // Data quality findings (move to tooltip, not shown in Findings section)
    const dataQualityFindings = [];
    if (dwellCoveragePct < 60) {
      dataQualityFindings.push({ level: 'yellow', text: `Dwell time data: ${dwellCoveragePct}% of records have complete start/end timestamps.` });
      recs.push('Confirm dwell start/end timestamps are being recorded consistently (workflow + integrations).');
    }
    if (processCoveragePct < 60) {
      dataQualityFindings.push({ level: 'yellow', text: `Process time data: ${processCoveragePct}% of records have complete start/end timestamps.` });
      recs.push('Confirm process start/end timestamps are being recorded consistently (dock door module usage).');
    }

    // Trend analysis: Dwell time (monthly -> weekly -> daily fallback)
    const dwellTrend = computeTrendAnalysis(
      { monthly: this.dwellByMonth, weekly: this.dwellByWeek, daily: this.dwellByDay },
      'Median dwell time',
      { significantChangePct: 15 }
    );
    if (dwellTrend) {
      if (dwellTrend.isSignificant) {
        const finding = formatTrendFinding(dwellTrend, {
          unit: ' min',
          increaseLevel: 'yellow',  // Longer dwell is concerning
          decreaseLevel: 'green',    // Shorter dwell is good
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);
      } else {
        // Report stable trend as well
        findings.push({
          level: 'green',
          text: `Median dwell time stable at ~${Math.round(dwellTrend.current.value)} min ${dwellTrend.granularityLabel}.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isTrendBased: true })
        });
      }
    }

    // Trend analysis: Process time (monthly -> weekly -> daily fallback)
    const processTrend = computeTrendAnalysis(
      { monthly: this.processByMonth, weekly: this.processByWeek, daily: this.processByDay },
      'Median process time',
      { significantChangePct: 15 }
    );
    if (processTrend) {
      if (processTrend.isSignificant) {
        const finding = formatTrendFinding(processTrend, {
          unit: ' min',
          increaseLevel: 'yellow',
          decreaseLevel: 'green',
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);
      } else {
        findings.push({
          level: 'green',
          text: `Median process time stable at ~${Math.round(processTrend.current.value)} min ${processTrend.granularityLabel}.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isTrendBased: true })
        });
      }
    }

    // Adoption concern: move_requested_by dominated by admins (business finding, keep)
    const topReq = this.moveRequestedBy.top(5);
    const totalReq = Array.from(this.moveRequestedBy.map.values()).reduce((a, b) => a + b, 0);
    const adminLike = topReq
      .filter(x => /admin|system|yms|super/i.test(x.key))
      .reduce((a, b) => a + b.value, 0);
    const adminShare = totalReq ? (adminLike / totalReq) : 0;

    if (totalReq >= 25 && adminShare >= 0.7) {
      findings.push({
        level: 'yellow',
        text: `Move requests appear dominated by admin/system users (~${Math.round(adminShare*100)}%).`,
        confidence: 'medium',
        confidenceReason: generateConfidenceReason('medium', { ...dqFactors, sampleSize: totalReq, isRatioBased: true })
      });
      recs.push('If end-user adoption is expected, review requester workflows, roles, and training (goal: requests driven by ops users).');
    }

    const dqBase = this.dataQualityScore();
    const dq = Math.round(0.5 * dqBase + 0.25 * dwellCoveragePct + 0.25 * processCoveragePct);
    const badge = scoreToBadge(dq);

    // Generate tooltip text for confidence badge
    const tooltipText = generateTooltipText('dockdoor_history', {
      score: dq,
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      dwellCoveragePct,
      processCoveragePct
    });

    // Pick best granularity for dwell/process charts
    const dwellGranularity = pickBestQuantileGranularity({
      monthly: this.dwellByMonth,
      weekly: this.dwellByWeek,
      daily: this.dwellByDay
    });
    const processGranularity = pickBestQuantileGranularity({
      monthly: this.processByMonth,
      weekly: this.processByWeek,
      daily: this.processByDay
    });

    // Use the finer of the two granularities for the combined chart
    // This ensures short data ranges (e.g., 1 week) use daily/weekly instead of falling back to monthly
    const granularityOrder = { month: 0, week: 1, day: 2 };
    const useGranularity = granularityOrder[dwellGranularity.granularity] >= granularityOrder[processGranularity.granularity]
      ? dwellGranularity : processGranularity;

    const dwellData = useGranularity.granularity === 'day' ? this.dwellByDay
      : useGranularity.granularity === 'week' ? this.dwellByWeek : this.dwellByMonth;
    const processData = useGranularity.granularity === 'day' ? this.processByDay
      : useGranularity.granularity === 'week' ? this.processByWeek : this.processByMonth;

    const dwellSeries = quantileSeriesFromMap(dwellData);
    const processSeries = quantileSeriesFromMap(processData);
    const timeLabel = useGranularity.granularity;
    const chartGranularityLabel = useGranularity.label;

    // Detect outliers in dwell and process time series
    const dwellOutliers = detectOutliersIQR(dwellSeries.labels, dwellSeries.median);
    const processOutliers = detectOutliersIQR(processSeries.labels, processSeries.median);

    const requesterTop = this.moveRequestedBy.top(8);
    const processorTop = this.processedBy.top(8);

    // Determine which leaderboard to show based on data availability
    // In CSV mode, we typically have processed_by but not move_requested_by
    const hasProcessedBy = processorTop.length > 0 && processorTop.reduce((a, b) => a + b.value, 0) > 0;
    const hasRequestedBy = requesterTop.length > 0 && requesterTop.reduce((a, b) => a + b.value, 0) > 0;

    // Choose the leaderboard with more data, preferring processed_by if equal
    const useProcessedByChart = hasProcessedBy && (!hasRequestedBy || this.processedBy.map.size >= this.moveRequestedBy.map.size);
    const leaderboardTop = useProcessedByChart ? processorTop : requesterTop;
    const leaderboardTitle = useProcessedByChart ? 'Top Processed By counts' : 'Top move_requested_by counts';
    const leaderboardField = useProcessedByChart ? 'processed_by' : 'move_requested_by';
    const leaderboardDesc = useProcessedByChart
      ? 'Helps track who is processing dock door events.'
      : 'Helps infer module adoption (admin vs others).';

    // Pick best granularity for door turns/utilization chart
    const turnsGranularity = pickBestGranularity({
      monthly: this.turnsByMonth,
      weekly: this.turnsByWeek,
      daily: this.turnsByDay
    });

    // Build turns time series data
    const turnsData = turnsGranularity.data;
    const turnsTimeLabel = turnsGranularity.granularity;
    const turnsChartGranularityLabel = turnsGranularity.label;

    // Get the appropriate doors data for the same granularity
    const doorsData = turnsTimeLabel === 'day' ? this.doorsByDay
      : turnsTimeLabel === 'week' ? this.doorsByWeek : this.doorsByMonth;

    // Build sorted labels from turns data
    const turnsLabels = turnsData ? [...turnsData.map.keys()].sort() : [];
    const turnsCounts = turnsLabels.map(k => turnsData.map.get(k) || 0);
    const doorsCounts = turnsLabels.map(k => doorsData.get(k)?.size || 0);

    // Add outlier findings if detected
    if (dwellOutliers.hasOutliers) {
      const outlierCount = dwellOutliers.outlierLabels.length;
      const outlierDates = dwellOutliers.outlierLabels.slice(0, 3).join(', ') +
        (outlierCount > 3 ? ` (+${outlierCount - 3} more)` : '');
      const maxOutlier = Math.max(...dwellOutliers.outlierValues);
      const maxOutlierHours = Math.round(maxOutlier / 60 * 10) / 10;

      findings.push({
        level: 'yellow',
        text: `Detected ${outlierCount} outlier ${timeLabel}(s) with unusually high dwell times: ${outlierDates}. ` +
          `Peak: ${Math.round(maxOutlier).toLocaleString()} min (~${maxOutlierHours.toLocaleString()} hrs). ` +
          `Median excluding outliers: ~${Math.round(dwellOutliers.medianWithoutOutliers)} min ` +
          `(vs ~${Math.round(dwellOutliers.medianWithOutliers)} min with outliers).`,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', {
          ...dqFactors,
          isThresholdBased: true
        })
      });
      recs.push('Investigate trailers with extended dwell times (>24 hrs). Common causes: storage trailers, missed check-outs, or workflow gaps.');
    }

    if (processOutliers.hasOutliers) {
      const outlierCount = processOutliers.outlierLabels.length;
      const outlierDates = processOutliers.outlierLabels.slice(0, 3).join(', ') +
        (outlierCount > 3 ? ` (+${outlierCount - 3} more)` : '');
      const maxOutlier = Math.max(...processOutliers.outlierValues);
      const maxOutlierHours = Math.round(maxOutlier / 60 * 10) / 10;

      findings.push({
        level: 'yellow',
        text: `Detected ${outlierCount} outlier ${timeLabel}(s) with unusually high process times: ${outlierDates}. ` +
          `Peak: ${Math.round(maxOutlier).toLocaleString()} min (~${maxOutlierHours.toLocaleString()} hrs). ` +
          `Median excluding outliers: ~${Math.round(processOutliers.medianWithoutOutliers)} min ` +
          `(vs ~${Math.round(processOutliers.medianWithOutliers)} min with outliers).`,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', {
          ...dqFactors,
          isThresholdBased: true
        })
      });
    }

    // Compute unified labels for the chart and map outlier indices
    const unifiedLabels = unionSorted(dwellSeries.labels, processSeries.labels);
    const dwellOutlierIndicesInUnified = dwellOutliers.outlierLabels
      .map(label => unifiedLabels.indexOf(label))
      .filter(idx => idx >= 0);
    const processOutlierIndicesInUnified = processOutliers.outlierLabels
      .map(label => unifiedLabels.indexOf(label))
      .filter(idx => idx >= 0);

    return {
      report: 'dockdoor_history',
      meta,
      inferredDateRange: this.getInferredDateRange(),
      dataQuality: {
        score: dq,
        ...badge,
        parseOk: this.parseOk,
        parseFails: this.parseFails,
        totalRows: this.totalRows,
        dwellCoveragePct,
        processCoveragePct,
        tooltipText,
        dataQualityFindings
      },
      metrics: {
        dwell_coverage_pct: dwellCoveragePct,
        process_coverage_pct: processCoveragePct,
        // A couple of rollups (latest month)
        dwell_median_latest_month_min: dwellSeries.median.at(-1) ?? null,
        dwell_p90_latest_month_min: dwellSeries.p90.at(-1) ?? null,
        process_median_latest_month_min: processSeries.median.at(-1) ?? null,
        process_p90_latest_month_min: processSeries.p90.at(-1) ?? null,
      },
      charts: [
        {
          id: `dwell_process_medians_${chartGranularityLabel}`,
          title: `Median dwell & process times (${chartGranularityLabel})`,
          kind: 'line',
          description: `Median minutes per ${timeLabel} (streaming quantile estimation).`,
          outlierInfo: {
            dwell: dwellOutliers,
            process: processOutliers
          },
          data: {
            labels: unifiedLabels,
            datasets: [
              { label: 'Dwell median (min)', data: alignSeries(unifiedLabels, dwellSeries.labels, dwellSeries.median), outlierIndices: dwellOutlierIndicesInUnified },
              { label: 'Process median (min)', data: alignSeries(unifiedLabels, processSeries.labels, processSeries.median), outlierIndices: processOutlierIndicesInUnified },
            ]
          },
          csv: {
            columns: [timeLabel, 'dwell_median_min', 'process_median_min', 'timezone'],
            rows: unifiedLabels.map((m, i) => ({
              [timeLabel]: m,
              dwell_median_min: alignSeries(unifiedLabels, dwellSeries.labels, dwellSeries.median)[i],
              process_median_min: alignSeries(unifiedLabels, processSeries.labels, processSeries.median)[i],
              timezone: meta.timezone
            }))
          }
        },
        {
          id: useProcessedByChart ? 'top_processed_by' : 'top_move_requested_by',
          title: leaderboardTitle,
          kind: 'bar',
          description: leaderboardDesc,
          data: {
            labels: leaderboardTop.map(x => x.key),
            datasets: [{ label: useProcessedByChart ? 'Events' : 'Requests', data: leaderboardTop.map(x => x.value) }]
          },
          csv: {
            columns: [leaderboardField, 'count'],
            rows: leaderboardTop.map(x => ({ [leaderboardField]: x.key, count: x.value }))
          }
        },
        // Door turns & utilization time series (only if we have data)
        ...(turnsLabels.length > 0 ? [{
          id: `door_turns_utilization_${turnsChartGranularityLabel}`,
          title: `Door turns & utilization (${turnsChartGranularityLabel})`,
          kind: 'line',
          description: `Number of door turns and unique doors utilized per ${turnsTimeLabel}.`,
          data: {
            labels: turnsLabels,
            datasets: [
              { label: 'Door turns', data: turnsCounts },
              { label: 'Doors utilized', data: doorsCounts }
            ]
          },
          csv: {
            columns: [turnsTimeLabel, 'door_turns', 'doors_utilized', 'timezone'],
            rows: turnsLabels.map((label, i) => ({
              [turnsTimeLabel]: label,
              door_turns: turnsCounts[i],
              doors_utilized: doorsCounts[i],
              timezone: meta.timezone
            }))
          }
        }] : [])
      ],
      findings,
      recommendations: recs,
      roi: computeDockDoorROIIfEnabled({
        meta,
        metrics: {
          turnsPerDoorPerDay: this.calculateTurnsPerDoorPerDay(),
          uniqueDoors: this.uniqueDoors.size,
          totalTurns: this.totalTurns,
          totalDays: this.daysWithData.size,
        },
        assumptions: meta.assumptions
      }),
    };
  }

  // Calculate average turns per door per day
  calculateTurnsPerDoorPerDay() {
    if (this.daysWithData.size === 0 || this.uniqueDoors.size === 0) return 0;

    // Sum up all turns per door per day, then average
    let totalDoorDays = 0;
    let totalTurnsCount = 0;

    for (const [, doorMap] of this.turnsByDoorByDay) {
      for (const [, turns] of doorMap) {
        totalDoorDays++;
        totalTurnsCount += turns;
      }
    }

    return totalDoorDays > 0 ? totalTurnsCount / totalDoorDays : 0;
  }
}

class DriverHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);

    this.movesTotal = 0;

    // Top drivers by move count
    this.movesByDriver = new CounterMap();

    // Week buckets: moves count + approx distinct drivers
    this.movesByWeek = new CounterMap();
    this.activeDriversByWeek = new Map(); // week -> ApproxDistinct

    // Compliance
    this.complianceOk = 0;
    this.complianceTotal = 0;

    // Queue time minutes quantiles
    this.queueMedian = new P2Quantile(0.5);
    this.queueP90 = new P2Quantile(0.9);
    this.queueTotal = 0;

    // Day boundaries: moves per day + approx distinct drivers per day
    this.movesByDay = new CounterMap();
    this.activeDriversByDay = new Map();

    // Track days worked per driver (for accurate per-day averages)
    this.daysWorkedByDriver = new Map(); // driver -> Set of day keys
  }

  getDistinct(map, key) {
    if (!map.has(key)) map.set(key, new ApproxDistinct(2048));
    return map.get(key);
  }

  getDaysWorked(driver) {
    if (!this.daysWorkedByDriver.has(driver)) {
      this.daysWorkedByDriver.set(driver, new Set());
    }
    return this.daysWorkedByDriver.get(driver);
  }

  ingest({ row }) {
    this.totalRows++;

    // Debug: Log first row to help diagnose field name issues
    if (this.totalRows === 1) {
      const keys = Object.keys(row).join(', ');
      this.warn(`driver_history: First row field names: ${keys}`);
    }

    const driver = safeStr(firstPresent(row, ['yard_driver_name', 'driver_name', 'driver', 'driver_username', 'driver_id']));

    // Determine if this is a completed move:
    // 1. Has a valid complete_time timestamp, OR
    // 2. Has event field indicating "Move has been finished"
    const completeRaw = firstPresent(row, ['complete_time', 'move_complete_time', 'completed_at', 'complete_timestamp']);
    const complete = parseTimestamp(completeRaw, {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });

    const event = safeStr(firstPresent(row, ['event', 'event_type', 'event_name']));
    const isMoveFinished = /move\s+has\s+been\s+finished|move\s+finished|finished/i.test(event);

    // Debug logging for first row
    if (this.totalRows === 1) {
      this.warn(`driver_history: First row - driver="${driver}", complete_time="${completeRaw}", event="${event}", isMoveFinished=${isMoveFinished}`);
    }

    // Count as a completed move if we have complete_time OR the event indicates completion
    const isCompletedMove = !!complete || isMoveFinished;

    if (isCompletedMove) {
      this.movesTotal++;
      if (driver) {
        this.movesByDriver.inc(driver);
      } else if (this.totalRows === 1) {
        // First row has no driver identifier - warn
        this.warn(`driver_history: No driver identifier found in first row. Expected fields: yard_driver_name, driver_name, driver, driver_username, or driver_id`);
      }
    }

    // Determine event time for grouping (complete time preferred, then start, then accept)
    const startRaw = firstPresent(row, ['start_time', 'move_start_time', 'started_at']);
    const start = parseTimestamp(startRaw, {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    const acceptRaw = firstPresent(row, ['accept_time', 'move_accept_time', 'accepted_at']);
    const accept = parseTimestamp(acceptRaw, {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });

    const eventDt = complete || start || accept;
    if (eventDt) {
      this.parseOk++;
      this.trackDate(eventDt); // Track for date range inference
    } else if (!eventDt && (completeRaw || startRaw || acceptRaw)) {
      // We have timestamp data but failed to parse it - warn once per analyzer
      if (this.totalRows === 1) {
        this.warn(`driver_history: Timestamp parsing failed for first row. Fields found: complete=${completeRaw ? 'present' : 'missing'}, start=${startRaw ? 'present' : 'missing'}, accept=${acceptRaw ? 'present' : 'missing'}`);
      }
    }

    // Only aggregate into weekly/daily charts if this is a completed move with valid timestamp
    if (isCompletedMove && eventDt) {
      const wk = weekKey(eventDt, this.timezone);
      const dy = dayKey(eventDt, this.timezone);
      this.movesByWeek.inc(wk);
      this.movesByDay.inc(dy);
      if (driver) {
        this.getDistinct(this.activeDriversByWeek, wk).add(driver);
        this.getDistinct(this.activeDriversByDay, dy).add(driver);
        // Track which days each driver worked (for accurate per-day averages)
        this.getDaysWorked(driver).add(dy);
      }
    }

    // Compliance rule:
    // % moves where accept/start/complete within <=2 minutes OR elapsed_time_minutes <= 2.
    this.complianceTotal++;
    const elapsedMin = maybeNumber(firstPresent(row, ['elapsed_time_minutes', 'elapsed_minutes', 'move_elapsed_minutes']));
    let ok = false;
    if (Number.isFinite(elapsedMin)) ok = elapsedMin <= 2;
    else if (accept && complete) {
      const diff = complete.diff(accept, 'minutes').minutes;
      if (Number.isFinite(diff)) ok = diff <= 2;
    } else if (start && complete) {
      const diff = complete.diff(start, 'minutes').minutes;
      if (Number.isFinite(diff)) ok = diff <= 2;
    }
    if (ok) this.complianceOk++;

    // Queue time
    const q = maybeNumber(firstPresent(row, ['time_in_queue_minutes', 'queue_time_minutes', 'time_in_queue']));
    if (Number.isFinite(q) && q >= 0) {
      this.queueMedian.add(q);
      this.queueP90.add(q);
      this.queueTotal++;
    }
  }

  finalize(meta) {
    const compliancePct = this.complianceTotal ? Math.round((this.complianceOk / this.complianceTotal) * 1000) / 10 : null;

    // Pick best granularity for driver activity chart
    const movesGranularity = pickBestGranularity({
      weekly: this.movesByWeek,
      daily: this.movesByDay
    });

    // Get labels and data based on chosen granularity
    let timeLabels, movesData, activeData, timeLabel, chartGranularityLabel;
    if (movesGranularity.granularity === 'day') {
      timeLabels = Array.from(this.activeDriversByDay.keys()).sort();
      movesData = timeLabels.map(d => this.movesByDay.map.get(d) || 0);
      activeData = timeLabels.map(d => this.activeDriversByDay.get(d)?.estimate() || 0);
      timeLabel = 'day';
      chartGranularityLabel = 'daily';
    } else {
      timeLabels = Array.from(this.activeDriversByWeek.keys()).sort();
      movesData = timeLabels.map(w => this.movesByWeek.map.get(w) || 0);
      activeData = timeLabels.map(w => this.activeDriversByWeek.get(w)?.estimate() || 0);
      timeLabel = 'week';
      chartGranularityLabel = 'weekly';
    }

    const topDrivers = this.movesByDriver.top(10);

    // Diagnostic warnings if no data was captured
    if (this.totalRows > 0 && this.movesByDriver.map.size === 0) {
      this.warn(`driver_history: Processed ${this.totalRows} rows but found no driver identifiers. Chart "Top drivers by moves" will be empty.`);
    }
    if (this.totalRows > 0 && timeLabels.length === 0) {
      this.warn(`driver_history: Processed ${this.totalRows} rows but no valid timestamps were parsed. Chart "Active drivers & moves" will be empty. Parse stats: ${this.parseOk} OK, ${this.parseFails} failed.`);
    }

    // Data quality findings (move to tooltip)
    const dataQualityFindings = [];
    if (compliancePct !== null && compliancePct < 30) {
      dataQualityFindings.push({ level: 'yellow', text: `Low compliance signal: ${compliancePct}% within ≤2 minutes.` });
    }

    // Data quality factors for confidence reasons
    const dqFactors = {
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      sampleSize: this.totalRows,
      compliancePct
    };

    const findings = [];
    const recs = [];

    // Trend analysis: Weekly moves
    const movesTrend = computeTrendAnalysis(
      { weekly: this.movesByWeek, daily: this.movesByDay },
      'Weekly moves',
      { significantChangePct: 15 }
    );
    if (movesTrend) {
      if (movesTrend.isSignificant) {
        const finding = formatTrendFinding(movesTrend, {
          increaseLevel: 'green',   // More moves is generally good (activity)
          decreaseLevel: 'yellow',
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);
      } else {
        findings.push({
          level: 'green',
          text: `Move volume stable at ~${Math.round(movesTrend.current.value)} ${movesTrend.granularityLabel}.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isTrendBased: true })
        });
      }
    }

    // Queue time finding (business insight)
    const queueMed = this.queueMedian.value();
    const queueP90 = this.queueP90.value();
    if (Number.isFinite(queueMed)) {
      if (queueMed > 10) {
        findings.push({
          level: 'yellow',
          text: `Median queue time is ~${Math.round(queueMed)} min (p90 ~${Math.round(queueP90 || 0)} min).`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, sampleSize: this.queueTotal })
        });
        recs.push('Investigate bottlenecks (gate, dispatch, dock availability). Queue time is a classic "hidden tax."');
      } else {
        findings.push({
          level: 'green',
          text: `Queue time healthy: median ~${Math.round(queueMed)} min.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, sampleSize: this.queueTotal })
        });
      }
    }

    // Compliance recommendation (linked to data quality issue)
    if (compliancePct !== null && compliancePct < 30) {
      recs.push('Recommend retraining on driver workflow (accept/start/complete), and validate device connectivity + timestamp capture.');
    }

    const dqBase = this.dataQualityScore();
    const dq = Math.round(0.55 * dqBase + 0.45 * (this.complianceTotal ? 100 : 70));
    const badge = scoreToBadge(dq);

    // Generate tooltip text for confidence badge
    const tooltipText = generateTooltipText('driver_history', {
      score: dq,
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      compliancePct
    });

    return {
      report: 'driver_history',
      meta,
      inferredDateRange: this.getInferredDateRange(),
      dataQuality: {
        score: dq,
        ...badge,
        parseOk: this.parseOk,
        parseFails: this.parseFails,
        totalRows: this.totalRows,
        tooltipText,
        dataQualityFindings
      },
      metrics: {
        moves_total: this.movesTotal,
        compliance_pct: compliancePct,
        queue_median_minutes: Number.isFinite(queueMed) ? Math.round(queueMed * 10) / 10 : null,
        queue_p90_minutes: Number.isFinite(queueP90) ? Math.round(queueP90 * 10) / 10 : null,
        // Derived “moves per driver per day” (approx)
        avg_moves_per_driver_per_day: deriveMovesPerDriverPerDay(this.movesByDay, this.activeDriversByDay),
      },
      charts: [
        {
          id: 'top_drivers_by_moves',
          title: 'Top drivers by moves',
          kind: 'bar',
          description: 'Top 10 drivers (by move count).',
          data: {
            labels: topDrivers.map(x => x.key),
            datasets: [{ label: 'Moves', data: topDrivers.map(x => x.value) }]
          },
          csv: {
            columns: ['driver', 'moves'],
            rows: topDrivers.map(x => ({ driver: x.key, moves: x.value }))
          }
        },
        {
          id: `active_drivers_and_moves_${chartGranularityLabel}`,
          title: `Active drivers & moves (${chartGranularityLabel})`,
          kind: 'line',
          description: `${chartGranularityLabel.charAt(0).toUpperCase() + chartGranularityLabel.slice(1)} trend using approximate distinct counting (no raw driver lists stored).`,
          data: {
            labels: timeLabels,
            datasets: [
              { label: 'Active drivers (approx)', data: activeData },
              { label: 'Moves', data: movesData }
            ]
          },
          csv: {
            columns: [timeLabel, 'active_drivers_approx', 'moves', 'timezone'],
            rows: timeLabels.map((t, i) => ({
              [timeLabel]: t,
              active_drivers_approx: activeData[i],
              moves: movesData[i],
              timezone: meta.timezone
            }))
          }
        }
      ],
      findings,
      recommendations: recs,
      roi: computeLaborROIIfEnabled({
        meta,
        metrics: {
          movesTotal: this.movesTotal,
          avgMovesPerDriverPerDay: deriveMovesPerDriverPerDay(this.movesByDay, this.activeDriversByDay),
          // Additional data for driver efficiency analysis
          topDrivers: topDrivers, // top 10 drivers by moves
          movesByDriver: this.movesByDriver, // all driver move counts
          movesByDay: this.movesByDay, // moves per day
          activeDriversByDay: this.activeDriversByDay, // drivers per day
          daysWorkedByDriver: this.daysWorkedByDriver, // driver -> Set of day keys
          totalDays: this.movesByDay.map.size,
        },
        assumptions: meta.assumptions
      }),
    };
  }
}

class TrailerHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);

    this.lostCount = 0;
    this.lostByCarrier = new CounterMap();
    this.eventTypes = new CounterMap();

    // Multi-granularity time series for trend analysis (monthly -> weekly -> daily fallback)
    this.byWeek = new CounterMap();
    this.byMonth = new CounterMap();
    this.byDay = new CounterMap();

    // Error rate tracking for ROI analysis
    this.errorCounts = {
      trailer_marked_lost: 0,
      yard_check_insert: 0,
      spot_edited: 0,
      facility_edited: 0,
    };
    this.errorsByPeriod = new CounterMap(); // day -> total errors
    this.daysWithData = new Set();

    // Per-error-type time series for multi-line chart
    this.lostByDay = new CounterMap();
    this.yardCheckInsertByDay = new CounterMap();
    this.spotEditedByDay = new CounterMap();
    this.facilityEditedByDay = new CounterMap();
  }

  ingest({ row }) {
    this.totalRows++;

    const event = safeStr(firstPresent(row, ['event', 'event_type', 'event_name', 'event_string', 'action', 'status_change']));
    if (event) this.eventTypes.inc(event);

    const dt = parseTimestamp(firstPresent(row, ['event_time', 'created_at', 'timestamp', 'event_timestamp']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    if (dt) {
      this.parseOk++;
      this.trackDate(dt); // Track for date range inference
      this.daysWithData.add(dayKey(dt, this.timezone));
    }

    const carrier = row.scac ?? row.carrier_scac ?? row.scac_code ?? row.carrier;

    const isLost = /marked\s+lost|trailer\s+marked\s+lost|\blost\b/i.test(event);
    if (isLost) {
      this.lostCount++;
      this.errorCounts.trailer_marked_lost++;
      if (!isNil(carrier)) this.lostByCarrier.inc(carrier);
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.byWeek.inc(weekKey(dt, this.timezone));
        this.byMonth.inc(monthKey(dt, this.timezone));
        this.byDay.inc(dk);
        this.errorsByPeriod.inc(dk);
        this.lostByDay.inc(dk);
      }
    }

    // Track other error-indicating events
    const isYardCheckInsert = /yard\s*check\s*insert/i.test(event);
    if (isYardCheckInsert) {
      this.errorCounts.yard_check_insert++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.yardCheckInsertByDay.inc(dk);
      }
    }

    const isSpotEdited = /spot\s*edited/i.test(event);
    if (isSpotEdited) {
      this.errorCounts.spot_edited++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.spotEditedByDay.inc(dk);
      }
    }

    const isFacilityEdited = /facility\s*edited/i.test(event);
    if (isFacilityEdited) {
      this.errorCounts.facility_edited++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.facilityEditedByDay.inc(dk);
      }
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    // Data quality factors for confidence reasons
    const dqFactors = {
      parseOk: this.parseOk,
      parseFails: this.parseFails,
      sampleSize: this.totalRows
    };

    // Data quality findings (move to tooltip)
    const dataQualityFindings = [];
    if (this.lostCount === 0) {
      dataQualityFindings.push({ level: 'green', text: 'No "Trailer marked lost" events found.' });
      recs.push('If lost events are expected but missing, confirm event strings and report configuration match local processes.');
    }

    // Trend analysis: Lost events (monthly -> weekly -> daily fallback)
    const lostTrend = computeTrendAnalysis(
      { monthly: this.byMonth, weekly: this.byWeek, daily: this.byDay },
      'Lost trailer events',
      { significantChangePct: 25 }
    );
    if (lostTrend) {
      if (lostTrend.isSignificant) {
        const finding = formatTrendFinding(lostTrend, {
          increaseLevel: 'red',     // More lost is bad
          decreaseLevel: 'green',
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);
        if (lostTrend.direction === 'increased') {
          recs.push('Lost events trending up - investigate carrier handoffs and scan compliance.');
        }
      } else if (this.lostCount > 0) {
        findings.push({
          level: 'yellow',
          text: `Lost events stable at ~${Math.round(lostTrend.current.value)} ${lostTrend.granularityLabel}.`,
          confidence: 'medium',
          confidenceReason: generateConfidenceReason('medium', { ...dqFactors, isTrendBased: true })
        });
      }
    }

    // Volume finding when no trend available but events exist
    if (!lostTrend && this.lostCount > 0) {
      if (this.lostCount > 10) {
        findings.push({
          level: 'yellow',
          text: `Detected ${this.lostCount} "Trailer marked lost" events - potential chaos signal.`,
          confidence: 'high',
          confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: this.lostCount, isThresholdBased: true })
        });
        recs.push('Investigate top carriers and process handoffs causing location drift; tighten scan/check-in and yard check frequency.');
      } else {
        findings.push({
          level: 'green',
          text: `${this.lostCount} "Trailer marked lost" events detected.`,
          confidence: 'high',
          confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: this.lostCount })
        });
      }
    }

    // Top carriers by lost events finding
    const topCarriers = this.lostByCarrier.top(8);
    const top3Carriers = topCarriers.slice(0, 3);
    if (top3Carriers.length > 0 && this.lostCount > 0) {
      const carrierDetails = top3Carriers.map(c => {
        const pct = Math.round((c.value / this.lostCount) * 100);
        return `${c.key}: ${c.value} (${pct}%)`;
      }).join(', ');

      const level = top3Carriers[0].value > 5 ? 'yellow' : 'green';
      findings.push({
        level,
        text: `Top carriers by lost events: ${carrierDetails}. High lost counts may indicate parking issues or carriers not following gate instructions.`,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: this.lostCount })
      });

      if (top3Carriers[0].value > 10) {
        recs.push(`Review gate procedures with carrier ${top3Carriers[0].key} - they account for ${Math.round((top3Carriers[0].value / this.lostCount) * 100)}% of lost trailer events.`);
      }
    }

    const dq = this.dataQualityScore();
    const badge = scoreToBadge(dq);

    // Generate tooltip text for confidence badge
    const tooltipText = generateTooltipText('trailer_history', {
      score: dq,
      parseOk: this.parseOk,
      parseFails: this.parseFails
    });

    const topEvents = this.eventTypes.top(10);

    // Calculate total errors for the chart
    const totalErrors = this.errorCounts.trailer_marked_lost +
      this.errorCounts.yard_check_insert +
      this.errorCounts.spot_edited +
      this.errorCounts.facility_edited;

    // Build error events chart with all error types
    // Get all unique days with any errors
    const allDays = new Set([
      ...this.lostByDay.map.keys(),
      ...this.yardCheckInsertByDay.map.keys(),
      ...this.spotEditedByDay.map.keys(),
      ...this.facilityEditedByDay.map.keys(),
    ]);
    const sortedDays = Array.from(allDays).sort();

    // Build datasets for each error type
    const lostData = sortedDays.map(d => this.lostByDay.map.get(d) || 0);
    const yardCheckData = sortedDays.map(d => this.yardCheckInsertByDay.map.get(d) || 0);
    const spotEditedData = sortedDays.map(d => this.spotEditedByDay.map.get(d) || 0);
    const facilityEditedData = sortedDays.map(d => this.facilityEditedByDay.map.get(d) || 0);
    const totalData = sortedDays.map((d, i) => lostData[i] + yardCheckData[i] + spotEditedData[i] + facilityEditedData[i]);

    return {
      report: 'trailer_history',
      meta,
      inferredDateRange: this.getInferredDateRange(),
      dataQuality: {
        score: dq,
        ...badge,
        parseOk: this.parseOk,
        parseFails: this.parseFails,
        totalRows: this.totalRows,
        tooltipText,
        dataQualityFindings
      },
      metrics: {
        total_error_events: totalErrors,
        trailer_marked_lost: this.errorCounts.trailer_marked_lost,
        yard_check_insert: this.errorCounts.yard_check_insert,
        spot_edited: this.errorCounts.spot_edited,
        facility_edited: this.errorCounts.facility_edited,
      },
      charts: [
        {
          id: 'error_events_daily',
          title: 'Error Events (daily)',
          kind: 'line',
          description: 'Error-indicating events by type. Total errors shown as thick line.',
          multiLineConfig: {
            totalLineIndex: 4, // Index of the "Total errors" dataset
          },
          data: {
            labels: sortedDays,
            datasets: [
              { label: 'Trailer marked lost', data: lostData },
              { label: 'Yard check insert', data: yardCheckData },
              { label: 'Spot edited', data: spotEditedData },
              { label: 'Facility edited', data: facilityEditedData },
              { label: 'Total errors', data: totalData, borderWidth: 3 },
            ]
          },
          csv: {
            columns: ['day', 'trailer_marked_lost', 'yard_check_insert', 'spot_edited', 'facility_edited', 'total_errors', 'timezone'],
            rows: sortedDays.map((d, i) => ({
              day: d,
              trailer_marked_lost: lostData[i],
              yard_check_insert: yardCheckData[i],
              spot_edited: spotEditedData[i],
              facility_edited: facilityEditedData[i],
              total_errors: totalData[i],
              timezone: meta.timezone
            }))
          }
        },
        {
          id: 'top_carriers_lost_events',
          title: 'Top carriers by lost events',
          kind: 'bar',
          description: 'Carriers most associated with "lost" events.',
          data: {
            labels: topCarriers.map(x => x.key),
            datasets: [{ label: 'Lost events', data: topCarriers.map(x => x.value) }]
          },
          csv: {
            columns: ['carrier_scac', 'lost_events'],
            rows: topCarriers.map(x => ({ carrier_scac: x.key, lost_events: x.value }))
          }
        }
      ],
      findings,
      recommendations: recs,
      roi: computeTrailerErrorRateAnalysis({
        metrics: {
          errorCounts: this.errorCounts,
          errorsByPeriod: this.getErrorsByPeriodArray(),
          totalRows: this.totalRows,
          totalDays: this.daysWithData.size,
          granularity: 'day',
        },
      }),
      extras: {
        event_type_top10: topEvents
      }
    };
  }

  // Convert errorsByPeriod CounterMap to sorted array for trend analysis
  getErrorsByPeriodArray() {
    const entries = Array.from(this.errorsByPeriod.map.entries())
      .map(([period, count]) => ({ period, count }))
      .sort((a, b) => a.period.localeCompare(b.period));
    return entries;
  }
}

// ---------- Factory ----------
export function createAnalyzers({ timezone, startDate, endDate, assumptions, onWarning, isCSVMode = false }) {
  const base = { timezone, startDate, endDate, assumptions, onWarning };
  return {
    current_inventory: new CurrentInventoryAnalyzer(base),
    detention_history: new DetentionHistoryAnalyzer({ ...base, isCSVMode }),
    dockdoor_history: new DockDoorHistoryAnalyzer(base),
    driver_history: new DriverHistoryAnalyzer(base),
    trailer_history: new TrailerHistoryAnalyzer(base),
  };
}

// ---------- Utilities ----------
function firstPresent(obj, keys) {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, k) && !isNil(obj[k])) return obj[k];
  }
  return null;
}

function mergeMonthSeries(detMap, prevMap) {
  const det = detMap.map;
  const prev = prevMap.map;
  const all = new Set([...det.keys(), ...prev.keys()]);
  const labels = Array.from(all).sort();
  return {
    labels,
    detention: labels.map(m => det.get(m) || 0),
    prevented: labels.map(m => prev.get(m) || 0),
  };
}

function quantileSeriesFromMap(map) {
  const labels = Array.from(map.keys()).sort();
  return {
    labels,
    median: labels.map(k => round1(map.get(k)?.median?.value())),
    p90: labels.map(k => round1(map.get(k)?.p90?.value())),
  };
}

function round1(v) {
  return Number.isFinite(v) ? Math.round(v * 10) / 10 : null;
}

function unionSorted(a, b) {
  const set = new Set([...(a || []), ...(b || [])]);
  return Array.from(set).sort();
}

function alignSeries(unionLabels, labels, values) {
  const idx = new Map(labels.map((l, i) => [l, i]));
  return unionLabels.map(l => {
    const i = idx.get(l);
    return (i === undefined) ? null : values[i];
  });
}

function counterToSeries(counterMap) {
  const labels = Array.from(counterMap.map.keys()).sort();
  return { labels, values: labels.map(l => counterMap.map.get(l) || 0) };
}

/**
 * Picks the best granularity (monthly/weekly/daily) based on data density.
 * Prefers finer granularity when there's enough data points.
 * @param {object} options - { monthly, weekly, daily } - CounterMap or Map objects
 * @param {number} minPoints - Minimum data points for a granularity to be valid (default 1)
 * @returns {object} { data, granularity, label } where granularity is 'month'|'week'|'day'
 */
function pickBestGranularity({ monthly, weekly, daily }, minPoints = 1) {
  // Check daily first (finest granularity)
  // Use daily if we have data and it's not too cluttered (≤60 days)
  if (daily) {
    const dailySize = daily.map?.size ?? daily.size ?? 0;
    if (dailySize >= minPoints && dailySize <= 60) {
      return { data: daily, granularity: 'day', label: 'daily' };
    }
  }

  // Check weekly - use if daily is too cluttered or not available
  // Use weekly if we have data and it's not too cluttered (≤26 weeks)
  if (weekly) {
    const weeklySize = weekly.map?.size ?? weekly.size ?? 0;
    if (weeklySize >= minPoints && weeklySize <= 26) {
      return { data: weekly, granularity: 'week', label: 'weekly' };
    }
  }

  // Default to monthly for long time ranges or when finer granularity is unavailable
  if (monthly) {
    return { data: monthly, granularity: 'month', label: 'monthly' };
  }

  return { data: null, granularity: 'month', label: 'monthly' };
}

/**
 * Picks the best granularity for quantile series (Maps with P2Quantile estimators).
 * Prefers finer granularity when data supports it.
 */
function pickBestQuantileGranularity({ monthly, weekly, daily }, minPoints = 1) {
  // Check daily first (finest granularity)
  // Use daily if we have data and it's not too cluttered (≤60 days)
  if (daily && daily.size >= minPoints && daily.size <= 60) {
    return { data: daily, granularity: 'day', label: 'daily' };
  }

  // Check weekly - use if daily is too cluttered or not available
  if (weekly && weekly.size >= minPoints && weekly.size <= 26) {
    return { data: weekly, granularity: 'week', label: 'weekly' };
  }

  // Default to monthly for long time ranges or when finer granularity is unavailable
  return { data: monthly, granularity: 'month', label: 'monthly' };
}

function deriveMovesPerDriverPerDay(movesByDay, activeDriversByDay) {
  const days = Array.from(activeDriversByDay.keys()).sort();
  if (!days.length) return null;

  let sum = 0;
  let count = 0;

  for (const d of days) {
    const moves = movesByDay.map.get(d) || 0;
    const drivers = activeDriversByDay.get(d)?.estimate() || 0;
    if (drivers > 0) {
      sum += moves / drivers;
      count++;
    }
  }
  return count ? Math.round((sum / count) * 10) / 10 : null;
}

// ---------- ROI (MVP: conservative, labeled as estimates) ----------
function computeDetentionROIIfEnabled({ meta, metrics, assumptions }) {
  const a = assumptions || {};
  const enabled =
    Number.isFinite(a.detention_cost_per_hour) &&
    Number.isFinite(a.labor_fully_loaded_rate_per_hour) &&
    Number.isFinite(a.target_moves_per_driver_per_day);

  if (!enabled) return null;

  // MVP: prevented detention count * 1 hour saved (placeholder). This is explicitly an estimate.
  const prevented = metrics.prevented || 0;
  const estHoursSaved = prevented * 1.0;
  const estValue = estHoursSaved * a.detention_cost_per_hour;

  return {
    label: 'Detention avoidance estimate',
    assumptionsUsed: {
      detention_cost_per_hour: a.detention_cost_per_hour,
      prevented_detention_hours_saved_each: 1.0,
    },
    estimate: {
      prevented_detention_events: prevented,
      estimated_hours_saved: round1(estHoursSaved),
      estimated_value: Math.round(estValue * 100) / 100,
    },
    disclaimer: 'Estimate only. Assumes each prevented detention event corresponds to ~1 hour avoided. Replace with customer-specific model when available.',
  };
}

function computeLaborROIIfEnabled({ meta, metrics, assumptions }) {
  const a = assumptions || {};

  // Only require target moves for driver ROI
  const enabled = Number.isFinite(a.target_moves_per_driver_per_day);

  if (!enabled) return null;

  const target = a.target_moves_per_driver_per_day;
  const laborRate = a.labor_fully_loaded_rate_per_hour || 42; // Default $42/hr
  const driverDayHours = 8;

  // Get actual metrics
  const avg = metrics.avgMovesPerDriverPerDay;
  const totalMoves = metrics.movesTotal || 0;
  const topDrivers = metrics.topDrivers || [];
  const movesByDriver = metrics.movesByDriver;
  const movesByDay = metrics.movesByDay;
  const activeDriversByDay = metrics.activeDriversByDay;
  const daysWorkedByDriver = metrics.daysWorkedByDriver; // driver -> Set of day keys
  const totalDays = metrics.totalDays || 0;

  // If we don't have enough data
  if (!Number.isFinite(avg) && totalMoves === 0) {
    return {
      label: 'Driver Performance & Staffing Analysis',
      assumptionsUsed: {
        labor_fully_loaded_rate_per_hour: laborRate,
        target_moves_per_driver_per_day: target,
        driver_day_hours: driverDayHours,
      },
      estimate: null,
      insights: [],
      staffingAnalysis: null,
      disclaimer: 'Insufficient data to estimate driver performance (missing driver/day aggregation).',
    };
  }

  // Calculate performance vs target
  const performancePct = Number.isFinite(avg) ? Math.round((avg / target) * 100) : null;
  const gap = Number.isFinite(avg) ? Math.max(0, target - avg) : null;
  const surplus = Number.isFinite(avg) ? Math.max(0, avg - target) : null;

  // Time/money calculations
  let timeSavedPerDriverDay = null;
  let moneySavedPerDriverDay = null;
  let fteEquivalent = null;

  if (Number.isFinite(avg)) {
    if (avg >= target) {
      // Drivers exceeding target - calculate efficiency gain
      timeSavedPerDriverDay = round1((surplus / avg) * driverDayHours * 60);
      moneySavedPerDriverDay = Math.round(((surplus / avg) * driverDayHours) * laborRate * 100) / 100;
    } else {
      // Below target - show the gap
      const gapHours = (gap / target) * driverDayHours;
      timeSavedPerDriverDay = -round1(gapHours * 60);
      moneySavedPerDriverDay = -Math.round(gapHours * laborRate * 100) / 100;
    }

    if (totalMoves > 0) {
      fteEquivalent = round1(totalMoves / target);
    }
  }

  // ========== DRIVER EFFICIENCY ANALYSIS ==========
  const staffingAnalysis = {};
  const insights = [];

  // Calculate average drivers per day (more useful than unique drivers for staffing analysis)
  let avgDriversPerDay = null;
  if (activeDriversByDay && activeDriversByDay.size > 0) {
    let totalDriverDays = 0;
    for (const approxDistinct of activeDriversByDay.values()) {
      totalDriverDays += approxDistinct.estimate();
    }
    avgDriversPerDay = round1(totalDriverDays / activeDriversByDay.size);
    staffingAnalysis.avgDriversPerDay = avgDriversPerDay;
  }

  // 1. Overall performance insight
  if (Number.isFinite(performancePct)) {
    if (performancePct >= 100) {
      insights.push(`Drivers averaging ${performancePct}% of target (${round1(avg)} moves/day vs ${target} target)`);
    } else {
      insights.push(`Drivers at ${performancePct}% of target (${round1(avg)} moves/day vs ${target} target)`);
    }
  }

  // 2. Total workload context
  if (totalMoves > 0 && totalDays > 0) {
    const avgMovesPerDay = round1(totalMoves / totalDays);
    staffingAnalysis.avgMovesPerDay = avgMovesPerDay;
    staffingAnalysis.totalDays = totalDays;
    staffingAnalysis.totalMoves = totalMoves;

    // How many drivers would be needed at target rate?
    const driversNeededAtTarget = round1(avgMovesPerDay / target);
    staffingAnalysis.driversNeededAtTarget = driversNeededAtTarget;

    insights.push(`Total facility workload: ${avgMovesPerDay} moves/day over ${totalDays} days`);
    insights.push(`At target rate (${target}/day), you'd need ~${driversNeededAtTarget} drivers/day`);
  }

  // 3. Analyze top performers vs average
  if (topDrivers.length > 0 && movesByDriver && daysWorkedByDriver) {
    const topDriver = topDrivers[0];
    // Calculate top driver's avg per day based on days THEY actually worked, not total days
    const topDriverDaysWorked = daysWorkedByDriver.get(topDriver.key)?.size || 0;
    const topDriverMovesPerDay = topDriverDaysWorked > 0
      ? round1(topDriver.value / topDriverDaysWorked)
      : null;

    staffingAnalysis.topDriverName = topDriver.key;
    staffingAnalysis.topDriverTotalMoves = topDriver.value;
    staffingAnalysis.topDriverDaysWorked = topDriverDaysWorked;
    staffingAnalysis.topDriverAvgPerDay = topDriverMovesPerDay;

    // Compare top performer to average
    if (Number.isFinite(topDriverMovesPerDay) && Number.isFinite(avg) && topDriverMovesPerDay > avg) {
      const topVsAvgRatio = round1(topDriverMovesPerDay / avg);
      staffingAnalysis.topVsAvgRatio = topVsAvgRatio;

      if (topDriverMovesPerDay >= target) {
        insights.push(`Top performer "${topDriver.key}" averages ${topDriverMovesPerDay} moves/day over ${topDriverDaysWorked} days worked (${Math.round((topDriverMovesPerDay/target)*100)}% of target)`);
      } else {
        insights.push(`Top performer "${topDriver.key}" averages ${topDriverMovesPerDay} moves/day over ${topDriverDaysWorked} days worked`);
      }
    }

    // Calculate: if all drivers performed like top driver, how many would you need?
    if (totalMoves > 0 && totalDays > 0 && Number.isFinite(topDriverMovesPerDay) && topDriverMovesPerDay > 0 && avgDriversPerDay) {
      const driversNeededIfAllLikeTop = round1((totalMoves / totalDays) / topDriverMovesPerDay);
      staffingAnalysis.driversNeededIfAllLikeTop = driversNeededIfAllLikeTop;

      if (avgDriversPerDay > driversNeededIfAllLikeTop) {
        const excessDrivers = round1(avgDriversPerDay - driversNeededIfAllLikeTop);
        insights.push(`If all drivers performed like top performer, you'd need ~${driversNeededIfAllLikeTop}/day vs ~${avgDriversPerDay} avg drivers/day (${excessDrivers} fewer)`);
      }
    }
  }

  // 4. Analyze daily productivity correlation with driver count
  if (movesByDay && activeDriversByDay && movesByDay.map.size >= 3) {
    const dailyData = [];
    for (const [day, moves] of movesByDay.map.entries()) {
      const driverCount = activeDriversByDay.get(day)?.estimate() || 0;
      if (driverCount > 0) {
        dailyData.push({
          day,
          moves,
          drivers: driverCount,
          movesPerDriver: round1(moves / driverCount)
        });
      }
    }

    if (dailyData.length >= 3) {
      // Sort by driver count to compare low vs high staffing days
      dailyData.sort((a, b) => a.drivers - b.drivers);

      const lowStaffDays = dailyData.slice(0, Math.ceil(dailyData.length / 3));
      const highStaffDays = dailyData.slice(-Math.ceil(dailyData.length / 3));

      const avgProductivityLowStaff = round1(lowStaffDays.reduce((s, d) => s + d.movesPerDriver, 0) / lowStaffDays.length);
      const avgProductivityHighStaff = round1(highStaffDays.reduce((s, d) => s + d.movesPerDriver, 0) / highStaffDays.length);
      const avgDriversLowStaff = round1(lowStaffDays.reduce((s, d) => s + d.drivers, 0) / lowStaffDays.length);
      const avgDriversHighStaff = round1(highStaffDays.reduce((s, d) => s + d.drivers, 0) / highStaffDays.length);

      staffingAnalysis.productivityByStaffing = {
        lowStaffDays: {
          avgDrivers: avgDriversLowStaff,
          avgMovesPerDriver: avgProductivityLowStaff,
          sampleSize: lowStaffDays.length
        },
        highStaffDays: {
          avgDrivers: avgDriversHighStaff,
          avgMovesPerDriver: avgProductivityHighStaff,
          sampleSize: highStaffDays.length
        }
      };

      // Insight about productivity vs staffing levels
      if (avgProductivityLowStaff > avgProductivityHighStaff * 1.1) {
        const pctMore = Math.round(((avgProductivityLowStaff / avgProductivityHighStaff) - 1) * 100);
        insights.push(`Days with fewer drivers (~${avgDriversLowStaff}) are ${pctMore}% MORE productive per driver (${avgProductivityLowStaff} vs ${avgProductivityHighStaff} moves/driver)`);
        insights.push(`Consider: Too many drivers may mean not enough moves to go around`);
      } else if (avgProductivityHighStaff > avgProductivityLowStaff * 1.1) {
        const pctMore = Math.round(((avgProductivityHighStaff / avgProductivityLowStaff) - 1) * 100);
        insights.push(`Days with more drivers (~${avgDriversHighStaff}) are ${pctMore}% MORE productive per driver (${avgProductivityHighStaff} vs ${avgProductivityLowStaff} moves/driver)`);
      } else {
        insights.push(`Productivity per driver is consistent across staffing levels (~${avgProductivityLowStaff}-${avgProductivityHighStaff} moves/driver)`);
      }
    }
  }

  // 5. Staffing recommendation (use avg drivers per day, not unique drivers)
  if (staffingAnalysis.driversNeededAtTarget && avgDriversPerDay) {
    const needed = staffingAnalysis.driversNeededAtTarget;
    const current = avgDriversPerDay;

    staffingAnalysis.staffingDelta = round1(current - needed);
    staffingAnalysis.avgDriversPerDay = current;

    if (current > needed * 1.3) {
      insights.push(`Potential overstaffing: ~${current} drivers/day avg vs ~${needed} needed at target rate`);
    } else if (current < needed * 0.8) {
      insights.push(`Potential understaffing: ~${current} drivers/day avg vs ~${needed} needed at target rate`);
    }
  }

  return {
    label: 'Driver Performance & Staffing Analysis',
    assumptionsUsed: {
      labor_fully_loaded_rate_per_hour: laborRate,
      target_moves_per_driver_per_day: target,
      driver_day_hours: driverDayHours,
    },
    estimate: {
      avg_moves_per_driver_per_day: Number.isFinite(avg) ? round1(avg) : null,
      target_moves_per_driver_per_day: target,
      performance_vs_target_pct: performancePct,
      gap_moves_per_day: gap !== null ? round1(gap) : null,
      surplus_moves_per_day: surplus !== null && surplus > 0 ? round1(surplus) : null,
      total_moves: totalMoves || null,
      driver_days_equivalent: fteEquivalent,
      time_impact_minutes_per_driver_day: timeSavedPerDriverDay,
      money_impact_per_driver_day: moneySavedPerDriverDay,
    },
    staffingAnalysis,
    insights,
    disclaimer: 'Estimates based on target moves assumption. Staffing analysis uses approximate driver counts. Actual results vary by site conditions.',
  };
}

// ---------- Dock Door Throughput ROI ----------
function computeDockDoorROIIfEnabled({ meta, metrics, assumptions }) {
  const a = assumptions || {};
  const target = Number.isFinite(a.target_turns_per_door_per_day) ? a.target_turns_per_door_per_day : null;
  const costPerHour = Number.isFinite(a.cost_per_dock_door_hour) ? a.cost_per_dock_door_hour : null;
  const hoursPerDay = 8; // Assume 8-hour operating day

  // Get metrics from dock door analysis
  const { turnsPerDoorPerDay, uniqueDoors, totalTurns, totalDays } = metrics;

  // Always show analysis if we have turn data (no assumptions required for basic insights)
  if (!Number.isFinite(turnsPerDoorPerDay) || turnsPerDoorPerDay === 0) {
    return {
      label: 'Dock Door Throughput Analysis',
      assumptionsUsed: {
        target_turns_per_door_per_day: target,
        cost_per_dock_door_hour: costPerHour,
      },
      estimate: null,
      insights: ['Insufficient data to calculate dock door throughput analysis.'],
      disclaimer: 'Unable to compute - dock door turn data not available.',
    };
  }

  const insights = [];

  // Always show: average turns per door per day (no assumptions needed)
  insights.push(`Dock doors averaging ${round1(turnsPerDoorPerDay)} turns/door/day`);

  // Always show: totals summary (no assumptions needed)
  if (totalTurns && totalDays) {
    insights.push(`Total: ${totalTurns} turns over ${totalDays} days across ${uniqueDoors || '?'} doors`);
  }

  // Build estimate object - always include base metrics
  const estimate = {
    avg_turns_per_door_per_day: round1(turnsPerDoorPerDay),
    unique_doors: uniqueDoors,
    total_turns: totalTurns,
    total_days: totalDays,
  };

  // Target-dependent insights (only if target is provided)
  if (target !== null) {
    const performancePct = Math.round((turnsPerDoorPerDay / target) * 100);
    const gap = Math.max(0, target - turnsPerDoorPerDay);
    const surplus = Math.max(0, turnsPerDoorPerDay - target);

    // Update first insight to include target comparison
    insights[0] = `Dock doors averaging ${round1(turnsPerDoorPerDay)} turns/day vs ${target} target (${performancePct}%)`;

    estimate.target_turns_per_door_per_day = target;
    estimate.performance_vs_target_pct = performancePct;
    estimate.gap_turns_per_day = gap > 0 ? round1(gap) : null;
    estimate.surplus_turns_per_day = surplus > 0 ? round1(surplus) : null;

    // Gap/surplus insight (requires target)
    if (performancePct >= 100) {
      insights.push(`Exceeding target by ${round1(surplus)} turns/door/day`);
    } else {
      insights.push(`Below target by ${round1(gap)} turns/door/day`);
    }

    // Cost-based insights (requires BOTH target and costPerHour)
    if (costPerHour !== null) {
      const costPerTurn = (costPerHour * hoursPerDay) / target;
      const dailyGapValue = gap * costPerTurn * (uniqueDoors || 1);
      const dailySurplusValue = surplus * costPerTurn * (uniqueDoors || 1);

      estimate.daily_gap_value = gap > 0 ? round1(dailyGapValue) : null;
      estimate.daily_surplus_value = surplus > 0 ? round1(dailySurplusValue) : null;

      if (performancePct >= 100 && dailySurplusValue > 0) {
        insights.push(`Efficiency value: ~$${round1(dailySurplusValue)}/day in additional throughput`);
      } else if (dailyGapValue > 0) {
        insights.push(`Opportunity cost: ~$${round1(dailyGapValue)}/day in unrealized capacity`);
      }
    }
  }

  return {
    label: 'Dock Door Throughput Analysis',
    assumptionsUsed: {
      target_turns_per_door_per_day: target,
      cost_per_dock_door_hour: costPerHour,
      hours_per_day: hoursPerDay,
    },
    estimate,
    insights,
    disclaimer: target !== null
      ? 'Estimates based on target turns assumption. Actual dock productivity varies by facility layout, product mix, and scheduling.'
      : 'Analysis based on recorded dock door events. Add target turns/door/day to enable performance comparison.',
  };
}

// ---------- Trailer History Error Rate Analysis ----------
function computeTrailerErrorRateAnalysis({ metrics }) {
  const { errorCounts, errorsByPeriod, totalRows, totalDays, granularity } = metrics;

  // Error types tracked
  const errorTypes = [
    { key: 'trailer_marked_lost', label: 'Trailer marked lost', indicator: 'Gate check-out accuracy issues or carrier parking issues upon check-in' },
    { key: 'yard_check_insert', label: 'Yard check insert', indicator: 'Gate check-in accuracy issues' },
    { key: 'spot_edited', label: 'Spot edited', indicator: 'Yard driver YMS usage issues' },
    { key: 'facility_edited', label: 'Facility edited', indicator: 'Shuttle/gate/campus operation issues' },
  ];

  const totalErrors = errorTypes.reduce((sum, t) => sum + (errorCounts[t.key] || 0), 0);

  if (totalErrors === 0) {
    return {
      label: 'Error Rate Analysis',
      assumptionsUsed: {},
      estimate: {
        total_errors: 0,
        error_rate_trend: null,
      },
      insights: ['No error-indicating events detected in this period.'],
      errorBreakdown: [],
      disclaimer: 'Error events tracked: Trailer marked lost, Yard check insert, Spot edited, Facility edited.',
    };
  }

  const insights = [];
  insights.push(`Total error-indicating events: ${totalErrors}`);

  // Calculate error rate trend if we have period data
  let trendPct = null;
  let trendDirection = null;
  if (errorsByPeriod && errorsByPeriod.length >= 3) {
    const firstHalf = errorsByPeriod.slice(0, Math.floor(errorsByPeriod.length / 2));
    const secondHalf = errorsByPeriod.slice(Math.ceil(errorsByPeriod.length / 2));

    const firstAvg = firstHalf.reduce((s, p) => s + p.count, 0) / firstHalf.length;
    const secondAvg = secondHalf.reduce((s, p) => s + p.count, 0) / secondHalf.length;

    if (firstAvg > 0) {
      trendPct = Math.round(((secondAvg - firstAvg) / firstAvg) * 100);
      trendDirection = trendPct > 0 ? 'increased' : (trendPct < 0 ? 'decreased' : 'stable');

      if (Math.abs(trendPct) >= 10) {
        insights.push(`Error rate ${trendDirection} by ${Math.abs(trendPct)}% over the analysis period`);
      } else {
        insights.push('Error rate relatively stable over the analysis period');
      }
    }
  }

  // Build error breakdown
  const errorBreakdown = errorTypes
    .filter(t => (errorCounts[t.key] || 0) > 0)
    .map(t => ({
      type: t.label,
      count: errorCounts[t.key],
      pctOfTotal: Math.round((errorCounts[t.key] / totalErrors) * 100),
      indicator: t.indicator,
    }))
    .sort((a, b) => b.count - a.count);

  // Top error insight
  if (errorBreakdown.length > 0) {
    const top = errorBreakdown[0];
    insights.push(`Most common: "${top.type}" (${top.count} events, ${top.pctOfTotal}% of errors) - ${top.indicator}`);
  }

  return {
    label: 'Error Rate Analysis',
    assumptionsUsed: {},
    estimate: {
      total_errors: totalErrors,
      total_rows: totalRows,
      total_days: totalDays,
      error_rate_per_day: totalDays > 0 ? round1(totalErrors / totalDays) : null,
      error_rate_trend_pct: trendPct,
      error_rate_trend_direction: trendDirection,
    },
    insights,
    errorBreakdown,
    disclaimer: 'Error events indicate operational accuracy issues. High "Facility edited" may be expected in campus operations without inter-facility gates.',
  };
}

// ---------- Detention Spend Calculation ----------
function computeDetentionSpendIfEnabled({ metrics, assumptions }) {
  const a = assumptions || {};
  const costPerHour = a.detention_cost_per_hour;
  const hasCostAssumption = Number.isFinite(costPerHour);

  const { detentionEvents, totalDetentionHours, actualDetentionCount } = metrics;
  const trueDetentionCount = actualDetentionCount ?? 0;

  // PM Note should show when there are truly NO detention events at all
  // This is independent of whether the cost assumption was provided
  const showPMNote = trueDetentionCount === 0;

  // If no cost assumption AND no detention events, still show the PM note
  if (!hasCostAssumption && showPMNote) {
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: {},
      estimate: {},
      insights: [],
      zeroDetentionNote: true,
      disclaimer: 'Enter "Detention cost per hour" to calculate detention spend.',
    };
  }

  // If no cost assumption but detention events exist, don't show the section at all
  if (!hasCostAssumption) {
    return null;
  }

  const insights = [];

  // Detention events exist but none have departure times yet
  // This means trailers are still on the yard, currently in detention
  if (trueDetentionCount > 0 && (detentionEvents === 0 || totalDetentionHours === 0)) {
    insights.push(`${trueDetentionCount} trailers currently in detention (no departures yet - spend accruing)`);
    insights.push('Detention spend will be calculable once trailers depart and departure timestamps are recorded.');
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: { detention_cost_per_hour: costPerHour },
      estimate: {
        trailers_in_detention: trueDetentionCount,
        completed_detention_events: 0,
        total_detention_hours: 0,
        detention_spend: 0,
      },
      insights,
      zeroDetentionNote: false,
      disclaimer: 'Trailers with detention start but no departure are still on the yard. Cost shown reflects completed detention events only.',
    };
  }

  // No detention events at all - simplified message (finding is shown in Findings section)
  if (trueDetentionCount === 0) {
    insights.push('Detention spend this period: $0 (no detention events recorded)');
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: { detention_cost_per_hour: costPerHour },
      estimate: {
        detention_events: 0,
        total_detention_hours: 0,
        detention_spend: 0,
      },
      insights,
      zeroDetentionNote: false,
      disclaimer: '',
    };
  }

  // Normal case: we have detention events and can calculate spend
  const detentionSpend = (totalDetentionHours || 0) * costPerHour;

  insights.push(`Detention spend this period: $${Math.round(detentionSpend).toLocaleString()} (${detentionEvents} trailers, ${round1(totalDetentionHours)} total hours)`);
  insights.push(`Average detention duration: ${round1(totalDetentionHours / detentionEvents)} hours per event`);

  return {
    label: 'Detention Spend Analysis',
    assumptionsUsed: { detention_cost_per_hour: costPerHour },
    estimate: {
      detention_events: detentionEvents,
      total_detention_hours: round1(totalDetentionHours),
      detention_spend: Math.round(detentionSpend * 100) / 100,
      avg_detention_hours: round1(totalDetentionHours / detentionEvents),
    },
    insights,
    zeroDetentionNote: false,
    disclaimer: 'Detention spend calculated from detention_datetime to departure_datetime. Does not include pre-detention time.',
  };
}

// ========== PARTIAL PERIOD DETECTION ==========

/**
 * Infer the granularity from time labels (weeks, months, days).
 * @param {string[]} labels - Array of time labels
 * @returns {'week'|'month'|'day'|'unknown'}
 */
export function inferGranularityFromLabels(labels) {
  if (!labels || labels.length === 0) return 'unknown';

  const sample = labels[0];

  // Week format: "2024-W01" or "2024-W52"
  if (/^\d{4}-W\d{1,2}$/.test(sample)) return 'week';

  // Month format: "2024-01" or "Jan 2024" or "2024-Jan"
  if (/^\d{4}-\d{2}$/.test(sample)) return 'month';
  if (/^[A-Za-z]{3}\s+\d{4}$/.test(sample)) return 'month';
  if (/^\d{4}-[A-Za-z]{3}$/.test(sample)) return 'month';

  // Day format: "2024-01-15" or "01/15/2024"
  if (/^\d{4}-\d{2}-\d{2}$/.test(sample)) return 'day';
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(sample)) return 'day';

  return 'unknown';
}

/**
 * Get human-readable granularity label (plural).
 * @param {'week'|'month'|'day'|'unknown'} granularity
 * @returns {string}
 */
export function getGranularityLabel(granularity) {
  switch (granularity) {
    case 'week': return 'weeks';
    case 'month': return 'months';
    case 'day': return 'days';
    default: return 'periods';
  }
}

/**
 * Detect partial periods for a single data series using median-based threshold.
 * A period is considered "partial" if its value is < 50% of the median of interior values.
 *
 * @param {string[]} labels - Time labels
 * @param {number[]} values - Data values corresponding to labels
 * @returns {{firstPartial: boolean, lastPartial: boolean, firstLabel: string|null, lastLabel: string|null}}
 */
export function detectPartialPeriodsForSeries(labels, values) {
  const result = {
    firstPartial: false,
    lastPartial: false,
    firstLabel: labels.length > 0 ? labels[0] : null,
    lastLabel: labels.length > 0 ? labels[labels.length - 1] : null,
  };

  if (!labels || !values || labels.length < 3) {
    // Not enough data points to determine partial periods
    return result;
  }

  // Get interior values (excluding first and last)
  const interiorValues = values.slice(1, -1).filter(v => Number.isFinite(v) && v > 0);

  if (interiorValues.length === 0) {
    return result;
  }

  // Calculate median of interior values
  const sorted = [...interiorValues].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];

  // Threshold: 50% of median
  const threshold = median * 0.5;

  // Check first and last values
  const firstVal = values[0];
  const lastVal = values[values.length - 1];

  if (Number.isFinite(firstVal) && firstVal < threshold) {
    result.firstPartial = true;
  }

  if (Number.isFinite(lastVal) && lastVal < threshold) {
    result.lastPartial = true;
  }

  return result;
}

/**
 * Interpolate value at a fractional index in a sorted array.
 * @param {number[]} arr - Sorted array of numbers
 * @param {number} idx - Fractional index
 * @returns {number}
 */
function interpolateAtIndex(arr, idx) {
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  if (lower === upper || upper >= arr.length) return arr[Math.min(lower, arr.length - 1)];
  const weight = idx - lower;
  return arr[lower] * (1 - weight) + arr[upper] * weight;
}

/**
 * Calculate median of an array of numbers.
 * @param {number[]} values - Array of numbers
 * @returns {number|null}
 */
function calculateMedianSimple(values) {
  if (!values || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

/**
 * Detect outliers in a time series using IQR method + absolute threshold.
 * A value is an outlier if:
 *   - It exceeds Q3 + 1.5*IQR (statistical outlier), OR
 *   - It exceeds the absolute threshold (e.g., 1440 min = 24 hours)
 *
 * @param {string[]} labels - Time labels
 * @param {number[]} values - Data values corresponding to labels
 * @param {object} options - Detection options
 * @param {number} options.absoluteThreshold - Absolute max value (default: 1440 min = 24hrs)
 * @param {number} options.iqrMultiplier - IQR multiplier for outlier fence (default: 1.5)
 * @returns {{
 *   hasOutliers: boolean,
 *   outlierIndices: number[],
 *   outlierLabels: string[],
 *   outlierValues: number[],
 *   q1: number|null,
 *   q3: number|null,
 *   iqr: number|null,
 *   upperFence: number|null,
 *   medianWithoutOutliers: number|null,
 *   medianWithOutliers: number|null
 * }}
 */
export function detectOutliersIQR(labels, values, options = {}) {
  const {
    absoluteThreshold = 1440, // 24 hours in minutes
    iqrMultiplier = 1.5
  } = options;

  const result = {
    hasOutliers: false,
    outlierIndices: [],
    outlierLabels: [],
    outlierValues: [],
    q1: null,
    q3: null,
    iqr: null,
    upperFence: null,
    medianWithoutOutliers: null,
    medianWithOutliers: null
  };

  if (!labels || !values || labels.length === 0) {
    return result;
  }

  // Filter valid values with their indices
  const validPairs = labels
    .map((label, i) => ({ label, value: values[i], index: i }))
    .filter(p => Number.isFinite(p.value) && p.value >= 0);

  if (validPairs.length < 4) {
    // Not enough data points for IQR-based detection
    return result;
  }

  // Sort values to compute quartiles
  const sortedValues = [...validPairs].map(p => p.value).sort((a, b) => a - b);
  const n = sortedValues.length;

  // Calculate Q1 and Q3 using linear interpolation
  const q1Idx = (n - 1) * 0.25;
  const q3Idx = (n - 1) * 0.75;

  const q1 = interpolateAtIndex(sortedValues, q1Idx);
  const q3 = interpolateAtIndex(sortedValues, q3Idx);
  const iqr = q3 - q1;
  const upperFence = q3 + iqrMultiplier * iqr;

  result.q1 = Math.round(q1 * 10) / 10;
  result.q3 = Math.round(q3 * 10) / 10;
  result.iqr = Math.round(iqr * 10) / 10;
  result.upperFence = Math.round(upperFence * 10) / 10;

  // Identify outliers
  const nonOutlierValues = [];

  for (const p of validPairs) {
    const isStatisticalOutlier = p.value > upperFence;
    const isAbsoluteOutlier = p.value > absoluteThreshold;

    if (isStatisticalOutlier || isAbsoluteOutlier) {
      result.outlierIndices.push(p.index);
      result.outlierLabels.push(p.label);
      result.outlierValues.push(p.value);
    } else {
      nonOutlierValues.push(p.value);
    }
  }

  result.hasOutliers = result.outlierIndices.length > 0;

  // Calculate medians with and without outliers
  result.medianWithOutliers = calculateMedianSimple(validPairs.map(p => p.value));
  result.medianWithoutOutliers = nonOutlierValues.length > 0
    ? calculateMedianSimple(nonOutlierValues)
    : result.medianWithOutliers;

  return result;
}

/**
 * Detect partial periods globally across all chart datasets from all results.
 * This ensures consistent trimming across all charts.
 *
 * @param {Object[]} allResults - Array of analysis results (from different reports)
 * @returns {{
 *   hasPartialPeriods: boolean,
 *   granularity: 'week'|'month'|'day'|'unknown',
 *   granularityLabel: string,
 *   firstPartial: {detected: boolean, label: string|null},
 *   lastPartial: {detected: boolean, label: string|null},
 *   detectedIn: string[]
 * }}
 */
export function detectGlobalPartialPeriods(allResults) {
  const global = {
    hasPartialPeriods: false,
    granularity: 'unknown',
    granularityLabel: 'periods',
    firstPartial: { detected: false, label: null },
    lastPartial: { detected: false, label: null },
    detectedIn: [], // Which charts/reports detected partial periods
  };

  if (!allResults || allResults.length === 0) {
    return global;
  }

  // Collect all time-series charts from all results
  const timeSeriesData = [];

  for (const result of allResults) {
    if (!result.charts) continue;

    for (const chart of result.charts) {
      // Only process line charts (time series)
      if (chart.kind !== 'line') continue;
      if (!chart.data?.labels || !chart.data?.datasets) continue;

      const labels = chart.data.labels;

      // Infer granularity from this chart's labels
      const chartGranularity = inferGranularityFromLabels(labels);
      if (chartGranularity !== 'unknown' && global.granularity === 'unknown') {
        global.granularity = chartGranularity;
        global.granularityLabel = getGranularityLabel(chartGranularity);
      }

      // Check each dataset in this chart
      for (const dataset of chart.data.datasets) {
        if (!dataset.data || dataset.data.length < 3) continue;

        const detection = detectPartialPeriodsForSeries(labels, dataset.data);

        timeSeriesData.push({
          chartId: chart.id,
          chartTitle: chart.title,
          report: result.report,
          labels,
          detection,
        });

        // Update global detection
        if (detection.firstPartial) {
          global.firstPartial.detected = true;
          if (!global.firstPartial.label) {
            global.firstPartial.label = detection.firstLabel;
          }
          global.detectedIn.push(`${chart.title} (first)`);
        }

        if (detection.lastPartial) {
          global.lastPartial.detected = true;
          if (!global.lastPartial.label) {
            global.lastPartial.label = detection.lastLabel;
          }
          global.detectedIn.push(`${chart.title} (last)`);
        }
      }
    }
  }

  global.hasPartialPeriods = global.firstPartial.detected || global.lastPartial.detected;

  return global;
}

/**
 * Apply partial period trimming/marking to chart data.
 * Returns a new chart data object with trimmed or marked data.
 *
 * @param {Object} chartData - Original chart data {labels, datasets}
 * @param {Object} globalPartialInfo - Result from detectGlobalPartialPeriods
 * @param {'include'|'trim'|'highlight'} mode - How to handle partial periods
 * @returns {Object} Modified chart data with partialPeriodInfo metadata
 */
export function applyPartialPeriodHandling(chartData, globalPartialInfo, mode = 'include') {
  if (!chartData?.labels || !chartData?.datasets) {
    return chartData;
  }

  const result = {
    labels: [...chartData.labels],
    datasets: chartData.datasets.map(ds => ({
      ...ds,
      data: [...ds.data],
    })),
    partialPeriodInfo: {
      mode,
      trimmedFirst: false,
      trimmedLast: false,
      highlightFirst: false,
      highlightLast: false,
      originalLength: chartData.labels.length,
    },
  };

  if (!globalPartialInfo.hasPartialPeriods || mode === 'include') {
    return result;
  }

  const { firstPartial, lastPartial } = globalPartialInfo;

  if (mode === 'trim') {
    // Remove partial periods from the data
    let startIdx = 0;
    let endIdx = result.labels.length;

    if (firstPartial.detected && result.labels[0] === firstPartial.label) {
      startIdx = 1;
      result.partialPeriodInfo.trimmedFirst = true;
    }

    if (lastPartial.detected && result.labels[result.labels.length - 1] === lastPartial.label) {
      endIdx = result.labels.length - 1;
      result.partialPeriodInfo.trimmedLast = true;
    }

    result.labels = result.labels.slice(startIdx, endIdx);
    result.datasets = result.datasets.map(ds => ({
      ...ds,
      data: ds.data.slice(startIdx, endIdx),
    }));

  } else if (mode === 'highlight') {
    // Mark partial periods for visual differentiation (dashed lines, hollow points)
    // The actual styling is handled in charts.js; here we just add metadata

    if (firstPartial.detected && result.labels[0] === firstPartial.label) {
      result.partialPeriodInfo.highlightFirst = true;
      result.partialPeriodInfo.firstIndex = 0;
    }

    if (lastPartial.detected && result.labels[result.labels.length - 1] === lastPartial.label) {
      result.partialPeriodInfo.highlightLast = true;
      result.partialPeriodInfo.lastIndex = result.labels.length - 1;
    }
  }

  return result;
}

/**
 * Recalculates ROI for existing results with new assumptions.
 * This allows updating ROI estimates without re-fetching/re-processing data.
 *
 * Since we don't store the internal streaming data structures, this function
 * recalculates only the assumption-dependent parts of the ROI (targets, costs, percentages)
 * while preserving the structural analysis from the original computation.
 *
 * @param {Object} results - The existing state.results object (report -> result)
 * @param {Object} assumptions - New assumptions to use for ROI calculations
 * @returns {Object} Updated results with recalculated ROI
 */
export function recalculateROI(results, assumptions) {
  const updated = {};

  for (const [report, result] of Object.entries(results)) {
    // Clone the result to avoid mutating state directly
    const newResult = { ...result };

    switch (report) {
      case 'detention_history':
        newResult.roi = recalcDetentionROI(result.roi, result.metrics, assumptions);
        // Recalculate detention spend using stored values from original computation
        newResult.detentionSpend = recalcDetentionSpend(result.detentionSpend, assumptions);
        break;

      case 'dockdoor_history':
        newResult.roi = recalcDockDoorROI(result.roi, result.metrics, assumptions);
        break;

      case 'driver_history':
        newResult.roi = recalcDriverROI(result.roi, result.metrics, assumptions);
        break;

      case 'trailer_history':
        // Trailer error analysis doesn't depend on assumptions, so keep original
        // but update the disclaimer if needed
        if (result.roi) {
          newResult.roi = { ...result.roi };
        }
        break;

      // current_inventory doesn't have ROI
      default:
        break;
    }

    updated[report] = newResult;
  }

  return updated;
}

/**
 * Recalculate detention ROI with new assumptions.
 * Uses stored metrics and original ROI estimate values.
 */
function recalcDetentionROI(existingRoi, metrics, assumptions) {
  if (!existingRoi) return null;

  const a = assumptions || {};
  const costPerHour = a.detention_cost_per_hour;

  if (!Number.isFinite(costPerHour)) return null;

  // Try multiple field names for prevented count (metrics uses prevented_detention_count, original ROI estimate uses prevented_detention_events)
  const prevented = metrics?.prevented_detention_count
    || existingRoi.estimate?.prevented_detention_events
    || 0;
  const hoursPerEvent = existingRoi.assumptionsUsed?.prevented_detention_hours_saved_each || 1;

  const preventedHours = prevented * hoursPerEvent;
  const savingsEstimate = preventedHours * costPerHour;

  return {
    label: 'Detention avoidance estimate',
    assumptionsUsed: {
      detention_cost_per_hour: costPerHour,
      prevented_detention_hours_saved_each: hoursPerEvent,
    },
    estimate: {
      prevented_detention_events: prevented,
      estimated_hours_saved: round1(preventedHours),
      estimated_value: Math.round(savingsEstimate * 100) / 100,
    },
    insights: [
      `${prevented} detention events prevented`,
      `~${round1(preventedHours)} detention hours avoided`,
      `Estimated savings: $${Math.round(savingsEstimate).toLocaleString()} at $${costPerHour}/hr`,
    ],
    disclaimer: 'Estimate only. Assumes each prevented detention event corresponds to ~1 hour avoided. Replace with customer-specific model when available.',
  };
}

/**
 * Recalculate dock door ROI with new assumptions.
 * Uses stored values from the original ROI estimate.
 * Now supports showing insights even without assumptions (matches original logic).
 */
function recalcDockDoorROI(existingRoi, metrics, assumptions) {
  if (!existingRoi) return null;

  const a = assumptions || {};
  const target = Number.isFinite(a.target_turns_per_door_per_day) ? a.target_turns_per_door_per_day : null;
  const costPerHour = Number.isFinite(a.cost_per_dock_door_hour) ? a.cost_per_dock_door_hour : null;
  const hoursPerDay = 8;

  // Get values from original ROI estimate (metrics don't have turns data)
  const avgTurns = existingRoi.estimate?.avg_turns_per_door_per_day || 0;
  const uniqueDoors = existingRoi.estimate?.unique_doors || 0;
  const totalDays = existingRoi.estimate?.total_days || 1;
  const totalTurns = existingRoi.estimate?.total_turns;

  // Need basic data to show any insights
  if (!Number.isFinite(avgTurns) || avgTurns === 0) {
    return existingRoi; // Keep original if no data
  }

  const insights = [];

  // Always show: average turns per door per day (no assumptions needed)
  insights.push(`Dock doors averaging ${round1(avgTurns)} turns/door/day`);

  // Always show: totals summary (no assumptions needed)
  if (totalTurns && totalDays) {
    insights.push(`Total: ${totalTurns} turns over ${totalDays} days across ${uniqueDoors || '?'} doors`);
  }

  // Build estimate object - always include base metrics
  const estimate = {
    avg_turns_per_door_per_day: round1(avgTurns),
    unique_doors: uniqueDoors,
    total_turns: totalTurns,
    total_days: totalDays,
  };

  // Target-dependent insights (only if target is provided)
  if (target !== null) {
    const performancePct = Math.round((avgTurns / target) * 100);
    const gap = Math.max(0, target - avgTurns);
    const surplus = Math.max(0, avgTurns - target);

    // Update first insight to include target comparison
    insights[0] = `Dock doors averaging ${round1(avgTurns)} turns/day vs ${target} target (${performancePct}%)`;

    estimate.target_turns_per_door_per_day = target;
    estimate.performance_vs_target_pct = performancePct;
    estimate.gap_turns_per_day = gap > 0 ? round1(gap) : null;
    estimate.surplus_turns_per_day = surplus > 0 ? round1(surplus) : null;

    // Gap/surplus insight (requires target)
    if (performancePct >= 100) {
      insights.push(`Exceeding target by ${round1(surplus)} turns/door/day`);
    } else {
      insights.push(`Below target by ${round1(gap)} turns/door/day`);
    }

    // Cost-based insights (requires BOTH target and costPerHour)
    if (costPerHour !== null) {
      const costPerTurn = (costPerHour * hoursPerDay) / target;
      const dailyGapValue = gap * costPerTurn * (uniqueDoors || 1);
      const dailySurplusValue = surplus * costPerTurn * (uniqueDoors || 1);

      estimate.daily_gap_value = gap > 0 ? round1(dailyGapValue) : null;
      estimate.daily_surplus_value = surplus > 0 ? round1(dailySurplusValue) : null;

      if (performancePct >= 100 && dailySurplusValue > 0) {
        insights.push(`Efficiency value: ~$${round1(dailySurplusValue)}/day in additional throughput`);
      } else if (dailyGapValue > 0) {
        insights.push(`Opportunity cost: ~$${round1(dailyGapValue)}/day in unrealized capacity`);
      }
    }
  }

  return {
    label: 'Dock Door Throughput Analysis',
    assumptionsUsed: {
      target_turns_per_door_per_day: target,
      cost_per_dock_door_hour: costPerHour,
      hours_per_day: hoursPerDay,
    },
    estimate,
    insights,
    disclaimer: target !== null
      ? 'Estimates based on target turns assumption. Actual dock productivity varies by facility layout, product mix, and scheduling.'
      : 'Analysis based on recorded dock door events. Add target turns/door/day to enable performance comparison.',
  };
}

/**
 * Recalculate driver ROI with new assumptions.
 * Preserves staffing analysis from original computation (which required streaming data).
 */
function recalcDriverROI(existingRoi, metrics, assumptions) {
  if (!existingRoi) return null;

  const a = assumptions || {};
  const target = a.target_moves_per_driver_per_day;
  const laborRate = a.labor_fully_loaded_rate_per_hour || 42;
  const driverDayHours = 8;

  if (!Number.isFinite(target)) return null;

  const avg = metrics?.avg_moves_per_driver_per_day || existingRoi.estimate?.avg_moves_per_driver_per_day || 0;
  const totalMoves = metrics?.moves_total || existingRoi.staffingAnalysis?.totalMoves || 0;
  const totalDays = existingRoi.staffingAnalysis?.totalDays || 1;
  const avgDriversPerDay = existingRoi.staffingAnalysis?.avgDriversPerDay;

  // Calculate performance vs target
  const performancePct = Number.isFinite(avg) && avg > 0 ? Math.round((avg / target) * 100) : null;
  const gap = Number.isFinite(avg) ? Math.max(0, target - avg) : null;
  const surplus = Number.isFinite(avg) ? Math.max(0, avg - target) : null;

  // Time/money calculations
  let moneySavedPerDriverDay = null;

  if (Number.isFinite(avg) && avg > 0) {
    if (avg >= target) {
      moneySavedPerDriverDay = Math.round(((surplus / avg) * driverDayHours) * laborRate * 100) / 100;
    } else {
      const gapHours = (gap / target) * driverDayHours;
      moneySavedPerDriverDay = -Math.round(gapHours * laborRate * 100) / 100;
    }
  }

  const fteEquivalent = totalMoves > 0 ? round1(totalMoves / target) : null;

  // Preserve existing staffing analysis but update target-dependent values
  const staffingAnalysis = existingRoi.staffingAnalysis ? { ...existingRoi.staffingAnalysis } : null;
  let driversNeededAtTarget = null;

  if (staffingAnalysis && totalMoves > 0 && totalDays > 0) {
    const avgMovesPerDay = round1(totalMoves / totalDays);
    driversNeededAtTarget = round1(avgMovesPerDay / target);
    staffingAnalysis.driversNeededAtTarget = driversNeededAtTarget;

    // Update staffing delta if we have avgDriversPerDay
    if (staffingAnalysis.avgDriversPerDay) {
      staffingAnalysis.staffingDelta = round1(staffingAnalysis.avgDriversPerDay - driversNeededAtTarget);
    }
  }

  // Build insights - preserve non-target-dependent insights from original, regenerate target-dependent ones
  const insights = [];

  // 1. Performance vs target (target-dependent - regenerate)
  if (Number.isFinite(performancePct)) {
    if (performancePct >= 100) {
      insights.push(`Drivers averaging ${performancePct}% of target (${round1(avg)} moves/day vs ${target} target)`);
    } else {
      insights.push(`Drivers at ${performancePct}% of target (${round1(avg)} moves/day vs ${target} target)`);
    }
  }

  // 2. Total workload context (data-dependent - use stored values)
  const avgMovesPerDay = staffingAnalysis?.avgMovesPerDay;
  if (avgMovesPerDay && totalDays > 0) {
    insights.push(`Total facility workload: ${avgMovesPerDay} moves/day over ${totalDays} days`);
  }

  // 3. Drivers needed at target (target-dependent - regenerate)
  if (driversNeededAtTarget && totalMoves > 0 && totalDays > 0) {
    insights.push(`At target rate (${target}/day), you'd need ~${driversNeededAtTarget} drivers/day`);
  }

  // 4. Preserve top performer insights (not target-dependent, but update the target % if mentioned)
  if (staffingAnalysis?.topDriverName && staffingAnalysis?.topDriverAvgPerDay) {
    const topDriverMovesPerDay = staffingAnalysis.topDriverAvgPerDay;
    const topDriverDaysWorked = staffingAnalysis.topDriverDaysWorked || 0;
    if (topDriverMovesPerDay >= target) {
      insights.push(`Top performer "${staffingAnalysis.topDriverName}" averages ${topDriverMovesPerDay} moves/day over ${topDriverDaysWorked} days worked (${Math.round((topDriverMovesPerDay/target)*100)}% of target)`);
    } else {
      insights.push(`Top performer "${staffingAnalysis.topDriverName}" averages ${topDriverMovesPerDay} moves/day over ${topDriverDaysWorked} days worked`);
    }
  }

  // 5. Preserve "if all like top performer" insight (target-independent)
  if (staffingAnalysis?.driversNeededIfAllLikeTop && avgDriversPerDay && staffingAnalysis?.topDriverAvgPerDay) {
    const driversNeededIfAllLikeTop = staffingAnalysis.driversNeededIfAllLikeTop;
    if (avgDriversPerDay > driversNeededIfAllLikeTop) {
      const excessDrivers = round1(avgDriversPerDay - driversNeededIfAllLikeTop);
      insights.push(`If all drivers performed like top performer, you'd need ~${driversNeededIfAllLikeTop}/day vs ~${avgDriversPerDay} avg drivers/day (${excessDrivers} fewer)`);
    }
  }

  // 6. Preserve productivity vs staffing insights (not target-dependent)
  if (staffingAnalysis?.productivityByStaffing) {
    const { lowStaffDays, highStaffDays } = staffingAnalysis.productivityByStaffing;
    if (lowStaffDays && highStaffDays) {
      const avgProductivityLowStaff = lowStaffDays.avgMovesPerDriver;
      const avgProductivityHighStaff = highStaffDays.avgMovesPerDriver;
      const avgDriversLowStaff = lowStaffDays.avgDrivers;
      const avgDriversHighStaff = highStaffDays.avgDrivers;

      if (avgProductivityLowStaff > avgProductivityHighStaff * 1.1) {
        const pctMore = Math.round(((avgProductivityLowStaff / avgProductivityHighStaff) - 1) * 100);
        insights.push(`Days with fewer drivers (~${avgDriversLowStaff}) are ${pctMore}% MORE productive per driver (${avgProductivityLowStaff} vs ${avgProductivityHighStaff} moves/driver)`);
        insights.push(`Consider: Too many drivers may mean not enough moves to go around`);
      } else if (avgProductivityHighStaff > avgProductivityLowStaff * 1.1) {
        const pctMore = Math.round(((avgProductivityHighStaff / avgProductivityLowStaff) - 1) * 100);
        insights.push(`Days with more drivers (~${avgDriversHighStaff}) are ${pctMore}% MORE productive per driver (${avgProductivityHighStaff} vs ${avgProductivityLowStaff} moves/driver)`);
      } else {
        insights.push(`Productivity per driver is consistent across staffing levels (~${avgProductivityLowStaff}-${avgProductivityHighStaff} moves/driver)`);
      }
    }
  }

  // 7. Staffing recommendation (target-dependent - regenerate)
  if (driversNeededAtTarget && avgDriversPerDay) {
    const needed = driversNeededAtTarget;
    const current = avgDriversPerDay;

    if (current > needed * 1.3) {
      insights.push(`Potential overstaffing: ~${current} drivers/day avg vs ~${needed} needed at target rate`);
    } else if (current < needed * 0.8) {
      insights.push(`Potential understaffing: ~${current} drivers/day avg vs ~${needed} needed at target rate`);
    }
  }

  return {
    label: 'Driver Performance & Staffing Analysis',
    assumptionsUsed: {
      labor_fully_loaded_rate_per_hour: laborRate,
      target_moves_per_driver_per_day: target,
      driver_day_hours: driverDayHours,
    },
    estimate: {
      avg_moves_per_driver_per_day: round1(avg),
      target_moves_per_driver_per_day: target,
      performance_vs_target_pct: performancePct,
      gap_moves_per_day: gap ? round1(gap) : null,
      surplus_moves_per_day: surplus ? round1(surplus) : null,
      money_impact_per_driver_day: moneySavedPerDriverDay,
      driver_days_equivalent: fteEquivalent,
    },
    insights,
    staffingAnalysis,
    disclaimer: 'Performance metrics based on average moves per driver per day. Individual driver performance may vary. Target rates should be calibrated to your operation.',
  };
}

/**
 * Recalculate detention spend with new assumptions.
 * Uses the stored metrics from the original computation.
 */
function recalcDetentionSpend(existingSpend, assumptions) {
  if (!existingSpend) return null;

  const a = assumptions || {};
  const costPerHour = a.detention_cost_per_hour;
  const hasCostAssumption = Number.isFinite(costPerHour);

  // Preserve the original zeroDetentionNote status (based on actual detention count)
  const wasZeroDetention = existingSpend.zeroDetentionNote;

  // If no cost assumption AND it was zero detention, keep showing PM note
  if (!hasCostAssumption && wasZeroDetention) {
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: {},
      estimate: {},
      insights: [],
      zeroDetentionNote: true,
      disclaimer: 'Enter "Detention cost per hour" to calculate detention spend.',
    };
  }

  // If no cost assumption but detention events existed, hide the section
  if (!hasCostAssumption) {
    return null;
  }

  // Get values from the stored estimate
  const detentionEvents = existingSpend.estimate?.detention_events ?? existingSpend.estimate?.detention_events_with_duration ?? 0;
  const totalDetentionHours = existingSpend.estimate?.total_detention_hours || 0;
  const detentionEventsFound = existingSpend.estimate?.detention_events_found;  // New field for "events without duration"

  // Recalculate spend with new cost
  const detentionSpend = totalDetentionHours * costPerHour;

  const insights = [];

  // Case: detention events exist but none have departed yet (still on yard)
  const trailersInDetention = existingSpend.estimate?.trailers_in_detention;
  if (trailersInDetention && trailersInDetention > 0 && (detentionEvents === 0 || totalDetentionHours === 0)) {
    insights.push(`${trailersInDetention} trailers currently in detention (no departures yet - spend accruing)`);
    insights.push('Detention spend will be calculable once trailers depart and departure timestamps are recorded.');
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: { detention_cost_per_hour: costPerHour },
      estimate: {
        trailers_in_detention: trailersInDetention,
        completed_detention_events: 0,
        total_detention_hours: 0,
        detention_spend: 0,
      },
      insights,
      zeroDetentionNote: false,
      disclaimer: 'Trailers with detention start but no departure are still on the yard. Cost shown reflects completed detention events only.',
    };
  }

  // Case: truly no detention events
  if (wasZeroDetention || (detentionEvents === 0 && totalDetentionHours === 0)) {
    insights.push('Detention spend this period: $0 (no detention events recorded)');
    return {
      label: 'Detention Spend Analysis',
      assumptionsUsed: { detention_cost_per_hour: costPerHour },
      estimate: {
        detention_events: 0,
        total_detention_hours: 0,
        detention_spend: 0,
      },
      insights,
      zeroDetentionNote: true,
      disclaimer: 'No detention events found in this data. Verify detention rules are configured in YMS.',
    };
  }

  // Normal case: we have detention events with duration
  insights.push(`Detention spend this period: $${Math.round(detentionSpend).toLocaleString()} (${detentionEvents} trailers, ${round1(totalDetentionHours)} total hours)`);
  insights.push(`Average detention duration: ${round1(totalDetentionHours / detentionEvents)} hours per event`);

  return {
    label: 'Detention Spend Analysis',
    assumptionsUsed: { detention_cost_per_hour: costPerHour },
    estimate: {
      detention_events: detentionEvents,
      total_detention_hours: round1(totalDetentionHours),
      detention_spend: Math.round(detentionSpend * 100) / 100,
      avg_detention_hours: round1(totalDetentionHours / detentionEvents),
    },
    insights,
    zeroDetentionNote: false,
    disclaimer: 'Detention spend calculated from detention_datetime to departure_datetime. Does not include pre-detention time.',
  };
}
