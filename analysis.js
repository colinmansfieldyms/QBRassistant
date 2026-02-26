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

/**
 * Normalizes dock door names for consistent unique counting.
 * Only counts actual dock doors (DR##, DOOR ##, D##, etc.) - returns empty string for
 * non-door locations (parking spots, staging areas, etc.) so they're not counted.
 * @param {string} doorStr - Raw door identifier string
 * @returns {string} Normalized door identifier, or empty string if not a dock door
 */
function normalizeDoorName(doorStr) {
  if (!doorStr) return '';
  let s = String(doorStr).trim().toUpperCase();

  // If the string contains " - " (e.g., "Zone A - Door 1"), extract the last part
  if (s.includes(' - ')) {
    const parts = s.split(' - ');
    s = parts[parts.length - 1].trim();
  }

  // Match dock door patterns: DR01, DR1, DOOR 1, DOOR1, D-1, D1, etc.
  // Captures the numeric part (with optional letter suffix like "1A")
  const doorMatch = s.match(/^(?:DOCK\s*)?(?:DOOR|DR|D)\s*-?\s*(\d+[A-Z]?)$/i);
  if (doorMatch) {
    // Normalize: pad single digits with leading zero for consistency (1 -> 01, but 10 stays 10)
    let num = doorMatch[1];
    if (/^\d$/.test(num)) {
      num = '0' + num;
    }
    return 'DOOR ' + num;
  }

  // If it's just a number (1-999, possibly with letter suffix), treat as door number
  const numMatch = s.match(/^(\d{1,3}[A-Z]?)$/);
  if (numMatch) {
    let num = numMatch[1];
    if (/^\d$/.test(num)) {
      num = '0' + num;
    }
    return 'DOOR ' + num;
  }

  // Not a recognized dock door pattern - return the normalized string as-is
  // This preserves other naming conventions (BAY 1, DOCK A, etc.) while still
  // normalizing case and trimming whitespace for deduplication
  return s;
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

// ---------- Detention Status Types ----------
/**
 * Detention status types for accurate detection.
 * These represent the ACTUAL status determined by comparing departure times against thresholds.
 */
const DETENTION_STATUS = {
  IN_DETENTION: 'IN_DETENTION',       // Departed after detention_start_time
  PREVENTED: 'PREVENTED',             // Departed after pre_detention but before detention
  NO_DETENTION: 'NO_DETENTION',       // Departed before pre_detention
  STILL_IN_YARD: 'STILL_IN_YARD',    // No departure yet
  UNKNOWN: 'UNKNOWN'                  // Missing threshold data
};

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
        lines.push(`Dwell data quality: ${factors.dwellCoveragePct}% of records have dwell timestamps`);
      }
      if (factors.processCoveragePct !== undefined) {
        lines.push(`YMS feature adoption: ${factors.processCoveragePct}% of dock visits used processing`);
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
    // Add R² explanations for high confidence trend analyses
    if (factors.r2 !== undefined && factors.r2 >= 0.7) {
      reasons.push(`strong linear trend (R²=${factors.r2}) indicates highly consistent pattern`);
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
      reasons.push(`${dwellCoveragePct}% of records have dwell timestamps`);
    }
    if (processCoveragePct !== null && processCoveragePct < 80) {
      reasons.push(`only ${processCoveragePct}% of dock visits used YMS processing feature`);
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
    if (isTrendBased && trendDataPoints !== null && trendDataPoints < 8) {
      reasons.push(`trend data has limited historical depth (${trendDataPoints} periods)`);
    }
    // Add R² explanations for trend analyses
    if (factors.r2 !== undefined) {
      if (factors.r2 >= 0.7) {
        reasons.push('strong linear trend (R²≥0.7) indicates consistent pattern');
      } else if (factors.r2 >= 0.4) {
        reasons.push('moderate trend fit (R²≥0.4) with some variation');
      }
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
    // Add R² explanations for low confidence trend analyses
    if (factors.r2 !== undefined && factors.r2 < 0.4) {
      reasons.push(`weak trend fit (R²=${factors.r2}) indicates high volatility`);
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
      granularityLabel: label,
      dataPointCount: n  // Total number of time periods available
    };
  }

  return null;
}

/**
 * Compare first half vs second half of the full time period.
 * For QBR use - shows if performance improved or degraded over the period.
 */
function computePeriodComparison(dataByGranularity, metricName, options = {}) {
  const { significantChangePct = 15, valueExtractor = null } = options;

  // Try granularities: monthly → weekly → daily
  const granularities = [
    { key: 'monthly', label: 'first half vs second half (monthly)', data: dataByGranularity.monthly },
    { key: 'weekly', label: 'first half vs second half (weekly)', data: dataByGranularity.weekly },
    { key: 'daily', label: 'first half vs second half (daily)', data: dataByGranularity.daily }
  ];

  for (const { key, label, data } of granularities) {
    if (!data) continue;

    const series = extractTimeSeries(data, { valueExtractor });
    if (!series || series.labels.length < 4) continue; // Need at least 4 periods to split

    // Split into two halves
    const midpoint = Math.floor(series.values.length / 2);
    const firstHalf = series.values.slice(0, midpoint);
    const secondHalf = series.values.slice(midpoint);

    // Calculate averages for each half
    const firstAvg = firstHalf.reduce((sum, v) => sum + v, 0) / firstHalf.length;
    const secondAvg = secondHalf.reduce((sum, v) => sum + v, 0) / secondHalf.length;

    if (!Number.isFinite(firstAvg) || !Number.isFinite(secondAvg) || firstAvg === 0) continue;

    const changePct = ((secondAvg - firstAvg) / Math.abs(firstAvg)) * 100;
    const direction = changePct > 0 ? 'increased' : 'decreased';
    const absChange = Math.abs(changePct);

    return {
      firstHalf: {
        value: firstAvg,
        periods: firstHalf.length,
        labels: series.labels.slice(0, midpoint)
      },
      secondHalf: {
        value: secondAvg,
        periods: secondHalf.length,
        labels: series.labels.slice(midpoint)
      },
      changePct: Math.round(changePct * 10) / 10,
      direction,
      isSignificant: absChange >= significantChangePct,
      metricName,
      granularity: key,
      granularityLabel: label,
      dataPointCount: series.values.length,
      analysisType: 'period-over-period'
    };
  }

  return null;
}

/**
 * Calculate overall trend using linear regression.
 * Returns slope, R² (fit quality), and plain-English interpretation.
 */
function computeOverallTrend(dataByGranularity, metricName, options = {}) {
  const { valueExtractor = null } = options;

  // Prefer monthly for overall trend (smoother, less noise)
  const granularities = [
    { key: 'monthly', label: 'over the period (monthly)', data: dataByGranularity.monthly },
    { key: 'weekly', label: 'over the period (weekly)', data: dataByGranularity.weekly },
    { key: 'daily', label: 'over the period (daily)', data: dataByGranularity.daily }
  ];

  for (const { key, label, data } of granularities) {
    if (!data) continue;

    const series = extractTimeSeries(data, { valueExtractor });
    if (!series || series.labels.length < 3) continue; // Need at least 3 points for regression

    // Simple linear regression: y = mx + b
    const n = series.values.length;
    const x = Array.from({ length: n }, (_, i) => i); // 0, 1, 2, ...
    const y = series.values;

    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((sum, xi, i) => sum + xi * y[i], 0);
    const sumX2 = x.reduce((sum, xi) => sum + xi * xi, 0);
    const sumY2 = y.reduce((sum, yi) => sum + yi * yi, 0);

    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;

    // Calculate R² (coefficient of determination)
    const yMean = sumY / n;
    const ssRes = y.reduce((sum, yi, i) => {
      const predicted = slope * x[i] + intercept;
      return sum + Math.pow(yi - predicted, 2);
    }, 0);
    const ssTot = y.reduce((sum, yi) => sum + Math.pow(yi - yMean, 2), 0);
    const r2 = 1 - (ssRes / ssTot);

    // Calculate percentage change from start to end
    // Use actual first/last values for display (regression can extrapolate to invalid negatives)
    const startValue = y[0];
    const endValue = y[n - 1];
    // Calculate trend change using actual values to avoid impossible percentages
    const trendChangePct = startValue !== 0
      ? ((endValue - startValue) / Math.abs(startValue)) * 100
      : 0;

    // Interpret trend stability based on R²
    let stability;
    if (r2 >= 0.7) {
      stability = 'consistent'; // Strong linear trend
    } else if (r2 >= 0.4) {
      stability = 'moderate'; // Some trend but with variation
    } else {
      stability = 'volatile'; // Weak trend, high variability
    }

    // Interpret direction
    let trendDirection;
    if (Math.abs(trendChangePct) < 5) {
      trendDirection = 'stable';
    } else if (trendChangePct > 0) {
      trendDirection = 'increasing';
    } else {
      trendDirection = 'decreasing';
    }

    return {
      slope: Math.round(slope * 100) / 100,
      intercept: Math.round(intercept * 10) / 10,
      r2: Math.round(r2 * 100) / 100,
      trendChangePct: Math.round(trendChangePct * 10) / 10,
      trendDirection,
      stability,
      metricName,
      granularity: key,
      granularityLabel: label,
      dataPointCount: n,
      averageValue: Math.round(yMean * 10) / 10,
      startValue: Math.round(startValue * 10) / 10,
      endValue: Math.round(endValue * 10) / 10,
      analysisType: 'overall-trend'
    };
  }

  return null;
}

/**
 * Identify best and worst performing periods.
 * Automatically detects if peaks/lows correlate with weekends and includes in analysis.
 */
function findPeakAndLowPeriods(dataByGranularity, metricName, options = {}) {
  const DateTime = getDateTime();
  const {
    valueExtractor = null,
    higherIsBetter = false   // False = lower is better (e.g., dwell time)
  } = options;

  // Use finest available granularity for detail
  const granularities = [
    { key: 'daily', data: dataByGranularity.daily },
    { key: 'weekly', data: dataByGranularity.weekly },
    { key: 'monthly', data: dataByGranularity.monthly }
  ];

  for (const { key, data } of granularities) {
    if (!data) continue;

    const series = extractTimeSeries(data, { valueExtractor });
    if (!series || series.labels.length < 2) continue;

    // Create pairs with day-of-week information (only for daily data)
    const pairs = series.labels.map((label, i) => {
      const dt = DateTime.fromISO(label);
      return {
        label,
        value: series.values[i],
        dayOfWeek: key === 'daily' ? dt.weekday : null, // 1=Mon, 7=Sun
        isWeekend: key === 'daily' ? (dt.weekday >= 6) : null
      };
    });

    if (pairs.length < 2) continue;

    // Sort by value
    const sorted = [...pairs].sort((a, b) => a.value - b.value);

    // Best and worst depend on whether higher or lower is better
    const best = higherIsBetter ? sorted[sorted.length - 1] : sorted[0];
    const worst = higherIsBetter ? sorted[0] : sorted[sorted.length - 1];

    // Calculate average
    const avg = pairs.reduce((sum, p) => sum + p.value, 0) / pairs.length;
    const bestVsAvgPct = ((best.value - avg) / Math.abs(avg)) * 100;
    const worstVsAvgPct = ((worst.value - avg) / Math.abs(avg)) * 100;

    // Analyze weekend patterns (only for daily data)
    let weekendPattern = null;
    if (key === 'daily') {
      // Get top 10 best and worst performers
      const topN = 10;
      const topBest = higherIsBetter
        ? sorted.slice(-topN).reverse()
        : sorted.slice(0, topN);
      const topWorst = higherIsBetter
        ? sorted.slice(0, topN)
        : sorted.slice(-topN).reverse();

      // Count how many are weekends
      const bestWeekendCount = topBest.filter(p => p.isWeekend).length;
      const worstWeekendCount = topWorst.filter(p => p.isWeekend).length;

      // If 60%+ of best/worst are weekends, flag it as a pattern
      if (bestWeekendCount >= topN * 0.6) {
        weekendPattern = {
          type: 'best',
          percentage: Math.round((bestWeekendCount / topN) * 100),
          message: `${bestWeekendCount} of top ${topN} ${higherIsBetter ? 'highest' : 'lowest'} periods are weekends`
        };
      } else if (worstWeekendCount >= topN * 0.6) {
        weekendPattern = {
          type: 'worst',
          percentage: Math.round((worstWeekendCount / topN) * 100),
          message: `${worstWeekendCount} of top ${topN} ${higherIsBetter ? 'lowest' : 'highest'} periods are weekends`
        };
      }

      // Also check if the single best/worst are weekends
      if (!weekendPattern) {
        if (best.isWeekend) {
          weekendPattern = {
            type: 'best-single',
            message: `${higherIsBetter ? 'Peak' : 'Best'} period occurred on a weekend`
          };
        } else if (worst.isWeekend) {
          weekendPattern = {
            type: 'worst-single',
            message: `${higherIsBetter ? 'Lowest' : 'Worst'} period occurred on a weekend`
          };
        }
      }
    }

    return {
      best: {
        label: best.label,
        value: Math.round(best.value * 10) / 10,
        vsAveragePct: Math.round(Math.abs(bestVsAvgPct) * 10) / 10,
        isWeekend: best.isWeekend,
        dayOfWeek: best.dayOfWeek
      },
      worst: {
        label: worst.label,
        value: Math.round(worst.value * 10) / 10,
        vsAveragePct: Math.round(Math.abs(worstVsAvgPct) * 10) / 10,
        isWeekend: worst.isWeekend,
        dayOfWeek: worst.dayOfWeek
      },
      average: Math.round(avg * 10) / 10,
      metricName,
      granularity: key,
      dataPointCount: pairs.length,
      analysisType: 'peak-low',
      higherIsBetter,
      weekendPattern // null or { type, percentage?, message }
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

  // Use the actual number of data points from the trend analysis
  const trendDataPoints = trend.dataPointCount || null;

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

/**
 * Format period-over-period comparison into a finding.
 */
function formatPeriodComparisonFinding(comparison, options = {}) {
  const {
    increaseLevel = 'yellow',
    decreaseLevel = 'green',
    unit = '',
    invertLevels = false,
    roundValues = true,
    dataQualityFactors = {}
  } = options;

  if (!comparison) return null;

  const absChange = Math.abs(comparison.changePct);
  const level = comparison.direction === 'increased'
    ? (invertLevels ? decreaseLevel : increaseLevel)
    : (invertLevels ? increaseLevel : decreaseLevel);

  const formatVal = (v) => {
    if (!Number.isFinite(v)) return '?';
    const val = roundValues ? Math.round(v * 10) / 10 : v;
    return `${val}${unit}`;
  };

  const firstVal = formatVal(comparison.firstHalf.value);
  const secondVal = formatVal(comparison.secondHalf.value);

  const confidenceReason = generateConfidenceReason('high', {
    ...dataQualityFactors,
    isTrendBased: true,
    trendDataPoints: comparison.dataPointCount
  });

  return {
    level,
    text: `${comparison.metricName} ${comparison.direction} ${absChange}% over the period (first half avg: ${firstVal}, second half avg: ${secondVal}).`,
    confidence: 'high',
    confidenceReason,
    analysisType: 'period-over-period'
  };
}

/**
 * Format overall trend analysis into a finding with plain-English explanation.
 */
function formatOverallTrendFinding(trend, options = {}) {
  const {
    increaseLevel = 'yellow',
    decreaseLevel = 'green',
    stableLevel = 'green',
    unit = '',
    invertLevels = false,
    roundValues = true,
    dataQualityFactors = {}
  } = options;

  if (!trend) return null;

  // Determine level based on trend direction
  let level;
  if (trend.trendDirection === 'stable') {
    level = stableLevel;
  } else if (trend.trendDirection === 'increasing') {
    level = invertLevels ? decreaseLevel : increaseLevel;
  } else {
    level = invertLevels ? increaseLevel : decreaseLevel;
  }

  const formatVal = (v) => {
    if (!Number.isFinite(v)) return '?';
    const val = roundValues ? Math.round(v * 10) / 10 : v;
    return `${val}${unit}`;
  };

  // Build plain-English description
  let stabilityText;
  if (trend.stability === 'consistent') {
    stabilityText = 'with consistent trend';
  } else if (trend.stability === 'moderate') {
    stabilityText = 'with some variation';
  } else {
    stabilityText = 'with high volatility';
  }

  const avgVal = formatVal(trend.averageValue);
  const absChange = Math.abs(trend.trendChangePct);

  let text;
  if (trend.trendDirection === 'stable') {
    text = `${trend.metricName} remained stable around ${avgVal} ${trend.granularityLabel} (${stabilityText}).`;
  } else {
    const startVal = formatVal(trend.startValue);
    const endVal = formatVal(trend.endValue);
    text = `${trend.metricName} ${trend.trendDirection} ${absChange}% ${trend.granularityLabel} (${startVal} → ${endVal}, ${stabilityText}).`;
  }

  const confidenceReason = generateConfidenceReason('high', {
    ...dataQualityFactors,
    isTrendBased: true,
    trendDataPoints: trend.dataPointCount,
    r2: trend.r2
  });

  return {
    level,
    text,
    confidence: 'high',
    confidenceReason,
    analysisType: 'overall-trend',
    metadata: {
      r2: trend.r2,
      stability: trend.stability
    }
  };
}

/**
 * Format peak/low period identification into a finding.
 * Includes weekend pattern analysis if detected.
 */
function formatPeakLowFinding(peakLow, options = {}) {
  const {
    unit = '',
    roundValues = true,
    dataQualityFactors = {},
    showBest = true,
    showWorst = true
  } = options;

  if (!peakLow) return null;

  const formatVal = (v) => {
    if (!Number.isFinite(v)) return '?';
    const val = roundValues ? Math.round(v * 10) / 10 : v;
    return `${val}${unit}`;
  };

  const bestLabel = peakLow.higherIsBetter ? 'Peak' : 'Best';
  const worstLabel = peakLow.higherIsBetter ? 'Lowest' : 'Worst';

  const parts = [];

  if (showBest) {
    const weekendNote = peakLow.best.isWeekend ? ' (weekend)' : '';
    parts.push(`${bestLabel}: ${formatVal(peakLow.best.value)} on ${peakLow.best.label}${weekendNote} (${peakLow.best.vsAveragePct}% ${peakLow.higherIsBetter ? 'above' : 'below'} avg)`);
  }

  if (showWorst) {
    const weekendNote = peakLow.worst.isWeekend ? ' (weekend)' : '';
    parts.push(`${worstLabel}: ${formatVal(peakLow.worst.value)} on ${peakLow.worst.label}${weekendNote} (${peakLow.worst.vsAveragePct}% ${peakLow.higherIsBetter ? 'below' : 'above'} avg)`);
  }

  let text = `${peakLow.metricName} - ${parts.join('; ')}.`;

  // Add weekend pattern observation if detected
  if (peakLow.weekendPattern) {
    text += ` Note: ${peakLow.weekendPattern.message}.`;
  }

  const confidenceReason = generateConfidenceReason('medium', {
    ...dataQualityFactors,
    isTrendBased: true,
    trendDataPoints: peakLow.dataPointCount
  });

  return {
    level: 'green', // Informational finding
    text,
    confidence: 'medium',
    confidenceReason,
    analysisType: 'peak-low',
    metadata: {
      weekendPattern: peakLow.weekendPattern
    }
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

// ---------- Facility Registry ----------
/**
 * Tracks unique facilities across all report types.
 * Used to determine if multi-facility mode should be enabled.
 */
class FacilityRegistry {
  constructor() {
    this.facilities = new Set();
    this.facilityByReport = new Map();
  }

  /**
   * Normalize and sanitize a facility name.
   * @param {string} facility - Raw facility name from data
   * @returns {string|null} - Normalized facility name, or null if invalid
   */
  static normalizeFacilityName(facility) {
    if (!facility || typeof facility !== 'string') return null;

    const trimmed = facility.trim();
    if (!trimmed || trimmed === '(blank)' || trimmed === '(unknown)' || trimmed === '') return null;

    // Length limit: max 100 characters to prevent layout issues
    let normalized = trimmed;
    if (normalized.length > 100) {
      console.warn(`Facility name exceeds 100 characters (${normalized.length}), truncating: ${normalized.slice(0, 50)}...`);
      normalized = normalized.slice(0, 100);
    }

    // Sanitize dangerous characters for XSS protection and HTML safety
    // Remove: < > " ' & to prevent attribute breaking and XSS
    // Keep: letters, numbers, spaces, hyphens, underscores, forward slash, parentheses, periods
    const original = normalized;
    normalized = normalized.replace(/[<>"'&]/g, '');

    if (normalized !== original) {
      console.warn(`Facility name contained special characters, sanitized: "${original}" -> "${normalized}"`);
    }

    // After sanitization, check if we have anything left
    normalized = normalized.trim();
    if (!normalized) {
      console.warn(`Facility name was empty after sanitization: "${original}"`);
      return null;
    }

    return normalized;
  }

  /**
   * Register a facility for a given report type.
   * @param {string} facility - The facility code/name
   * @param {string} reportType - The report type (e.g., 'dockdoor_history')
   */
  register(facility, reportType) {
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return;

    this.facilities.add(normalized);

    if (!this.facilityByReport.has(reportType)) {
      this.facilityByReport.set(reportType, new Set());
    }
    this.facilityByReport.get(reportType).add(normalized);
  }

  /**
   * Check if multiple facilities have been detected.
   * @returns {boolean}
   */
  isMultiFacility() {
    return this.facilities.size >= 2;
  }

  /**
   * Get all detected facilities, sorted alphabetically.
   * @returns {string[]}
   */
  getFacilities() {
    return Array.from(this.facilities).sort();
  }

  /**
   * Get facilities detected for a specific report type.
   * @param {string} reportType
   * @returns {string[]}
   */
  getFacilitiesForReport(reportType) {
    const set = this.facilityByReport.get(reportType);
    return set ? Array.from(set).sort() : [];
  }

  /**
   * Clear all tracked facilities. Call before starting a new analysis run.
   */
  clear() {
    this.facilities.clear();
    this.facilityByReport.clear();
  }
}

// Singleton instance for tracking facilities across the application
export const facilityRegistry = new FacilityRegistry();

// ---------- Report analyzers ----------
class BaseAnalyzer {
  constructor({ timezone, startDate, endDate, assumptions, onWarning, enableDrilldown = true }) {
    this.timezone = timezone;
    this.startDate = startDate;
    this.endDate = endDate;
    this.assumptions = assumptions;
    this.onWarning = onWarning;
    this.enableDrilldown = enableDrilldown;

    this.totalRows = 0;
    this.parseFails = 0;
    this.parseOk = 0;

    this.warnings = [];

    // Track date range from ingested data (for CSV mode)
    this.earliestDate = null; // DateTime object
    this.latestDate = null;   // DateTime object

    // Per-facility data storage for multi-facility support
    // Maps facility name → per-facility metrics bucket
    this.byFacility = new Map();
  }

  /**
   * Get or create a per-facility metrics bucket.
   * Subclasses should override createFacilityBucket() to define the bucket structure.
   * @param {string} facility - The facility identifier
   * @returns {object} The facility-specific metrics bucket
   */
  getOrCreateFacilityBucket(facility) {
    if (!facility) return null;
    if (!this.byFacility.has(facility)) {
      this.byFacility.set(facility, this.createFacilityBucket());
    }
    return this.byFacility.get(facility);
  }

  /**
   * Create a new per-facility metrics bucket.
   * Subclasses should override this to return appropriate metric containers.
   * @returns {object} Empty metrics bucket for a facility
   */
  createFacilityBucket() {
    // Default implementation - subclasses override
    return { totalRows: 0 };
  }

  /**
   * Register a facility with the global registry and get its bucket.
   * Call this in ingest() with the facility from the row.
   * @param {string} facility - The facility identifier
   * @param {string} reportType - The report type name
   * @returns {object|null} The facility bucket, or null if no facility
   */
  trackFacility(facility, reportType) {
    if (!facility) return null;
    // Normalize the facility name to ensure consistency between registry and bucket storage
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;
    facilityRegistry.register(normalized, reportType);
    return this.getOrCreateFacilityBucket(normalized);
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

  /**
   * Get list of facilities that have data in this analyzer.
   * @returns {string[]} Array of facility names
   */
  getFacilities() {
    return Array.from(this.byFacility.keys()).sort();
  }

  /**
   * Finalize results for a specific facility.
   * Subclasses should override this to produce facility-specific results.
   * @param {string} facility - The facility name
   * @param {Object} meta - Metadata for finalization
   * @returns {Object|null} The result object for this facility, or null if not supported
   */
  finalizeFacility(facility, meta) {
    // Default implementation returns null - subclasses override
    return null;
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

    // Drilldown data for yard-age buckets (only collected when enableDrilldown is true)
    this.yardAgeDrilldown = { '0-1d': [], '1-7d': [], '7-30d': [], '30d+': [] };

    this.moveType = new CounterMap();
    this.outbound = 0;
    this.inbound = 0;
    this.placeholderScac = 0;
    this.scacTotal = 0;

    this.liveLoads = 0;
    this.liveLoadMissingDriverContact = 0; // presence only
  }

  /**
   * Create a per-facility metrics bucket for current inventory.
   */
  createFacilityBucket() {
    return {
      totalRows: 0,
      totalTrailers: 0,
      yardAgeBuckets: { '0-1d': 0, '1-7d': 0, '7-30d': 0, '30d+': 0, 'unknown': 0 },
      moveType: new CounterMap(),
      outbound: 0,
      inbound: 0,
      placeholderScac: 0,
      scacTotal: 0,
      liveLoads: 0,
      liveLoadMissingDriverContact: 0,
    };
  }

  ingest({ row, flags }) {
    this.totalRows++;
    this.totalTrailers++;

    // Track facility for multi-facility support
    const facility = row._facility || flags?.facility || '';
    const facBucket = this.trackFacility(facility, 'current_inventory');
    if (facBucket) {
      facBucket.totalRows++;
      facBucket.totalTrailers++;
    }
    const DateTime = getDateTime();

    // move_type_name distribution
    const mt = safeStr(row.move_type_name);
    if (mt) {
      this.moveType.inc(mt);
      if (facBucket) facBucket.moveType.inc(mt);
    }
    const mtLower = mt.toLowerCase();
    if (mtLower.includes('out')) {
      this.outbound++;
      if (facBucket) facBucket.outbound++;
    }
    if (mtLower.includes('in')) {
      this.inbound++;
      if (facBucket) facBucket.inbound++;
    }

    // SCAC placeholder
    const scac = row.scac ?? row.carrier_scac ?? row.scac_code;
    if (!isNil(scac)) {
      this.scacTotal++;
      if (facBucket) facBucket.scacTotal++;
      if (scacIsPlaceholder(scac)) {
        this.placeholderScac++;
        if (facBucket) facBucket.placeholderScac++;
      }
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
      if (facBucket) facBucket.liveLoads++;
      if (!flags.driverContactPresent) {
        this.liveLoadMissingDriverContact++;
        if (facBucket) facBucket.liveLoadMissingDriverContact++;
      }
    }

    // CSV-specific: yard-age buckets from Elapsed Time (Hours)
    if (row.csv_elapsed_hours !== undefined && row.csv_elapsed_hours !== null && row.csv_elapsed_hours !== '') {
      this.hasCSVYardAge = true;
      const hours = parseFloat(row.csv_elapsed_hours);
      let bucket = null;
      if (Number.isFinite(hours) && hours >= 0) {
        if (hours <= 24) bucket = '0-1d';
        else if (hours <= 168) bucket = '1-7d';
        else if (hours <= 720) bucket = '7-30d';
        else bucket = '30d+';
        this.yardAgeBuckets[bucket]++;
        if (facBucket) facBucket.yardAgeBuckets[bucket]++;
      } else {
        this.yardAgeBuckets['unknown']++;
        if (facBucket) facBucket.yardAgeBuckets['unknown']++;
      }

      // Collect drilldown data if enabled
      if (bucket && this.enableDrilldown) {
        const trailer = safeStr(row.trailer_number || row.trailer_id || row.equipment_number || '');
        const scac = safeStr(row.scac ?? row.carrier_scac ?? row.scac_code ?? '');
        const moveType = safeStr(row.move_type_name || row.move_type || '');
        const location = safeStr(row.drop_spot || row.location_name || row.parking_spot || row.spot || '');
        const ageDays = Math.round(hours / 24 * 10) / 10;
        this.yardAgeDrilldown[bucket].push({ trailer, scac, moveType, ageDays, location });
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

      // Build drilldown data by bucket label
      const drilldown = this.enableDrilldown ? {} : null;
      if (drilldown) {
        yardAgeSeries.forEach(({ bucket }) => {
          drilldown[bucket] = this.yardAgeDrilldown[bucket] || [];
        });
      }

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
        },
        drilldown: drilldown ? {
          columns: ['trailer', 'scac', 'moveType', 'ageDays', 'location'],
          columnLabels: ['Trailer #', 'SCAC', 'Move Type', 'Age (days)', 'Location'],
          byLabel: drilldown
        } : null
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
        outbound_vs_inbound_ratio: outboundInboundRatio !== null ? Math.round(outboundInboundRatio * 100) / 100 : null,
        live_load_missing_driver_contact_pct: missingDriverRate,
      },
      charts: this.buildCharts(moveTypeTop, updatedSeries),
      findings,
      recommendations: recs,
      roi: null, // current inventory doesn't drive ROI directly in MVP
      exports: {
        reportSummaryCsv: null, // built in export.js on demand
      }
    };
  }

  /**
   * Finalize results for a specific facility.
   * Returns a simplified result with key metrics for tabbed display.
   */
  finalizeFacility(facility, meta) {
    // Normalize facility name to ensure consistent lookup with how buckets are stored
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;

    const bucket = this.byFacility.get(normalized);
    if (!bucket) return null;

    const pct = (n, d) => d ? Math.round((n / d) * 1000) / 10 : 0;
    const trailers = bucket.totalTrailers || 0;
    const placeholderRate = bucket.scacTotal ? pct(bucket.placeholderScac, bucket.scacTotal) : 0;
    const outboundInboundRatio = bucket.inbound ? (bucket.outbound / bucket.inbound) : null;
    const missingDriverRate = bucket.liveLoads ? pct(bucket.liveLoadMissingDriverContact, bucket.liveLoads) : null;

    // Simplified data quality
    const dq = bucket.totalRows > 0 ? 75 : 0; // Simplified score for facility view

    // Build simple charts from facility bucket
    const moveTypeTop = bucket.moveType.toObjectSorted();
    const charts = [];
    if (Object.keys(moveTypeTop).length > 0) {
      charts.push({
        id: 'move_type',
        kind: 'pie',
        title: `Move Types - ${facility}`,
        data: moveTypeTop,
      });
    }

    // Generate findings and recommendations for this facility
    const findings = [];
    const recs = [];

    // Outbound/inbound ratio findings
    if (outboundInboundRatio !== null) {
      if (outboundInboundRatio > 2) {
        findings.push({
          level: 'yellow',
          text: `${facility} has outbound-heavy inventory ratio (${outboundInboundRatio.toFixed(1)}:1 outbound to inbound).`,
          confidence: 'medium',
        });
        recs.push(`Review if outbound staging at ${facility} is backing up or if inbound flow is constrained.`);
      } else if (outboundInboundRatio < 0.5) {
        findings.push({
          level: 'yellow',
          text: `${facility} has inbound-heavy inventory ratio (${(1/outboundInboundRatio).toFixed(1)}:1 inbound to outbound).`,
          confidence: 'medium',
        });
      } else {
        findings.push({
          level: 'green',
          text: `${facility} has balanced outbound/inbound inventory ratio.`,
          confidence: 'medium',
        });
      }
    }

    // SCAC placeholder findings
    if (placeholderRate >= 10) {
      findings.push({
        level: 'yellow',
        text: `${placeholderRate}% of trailers at ${facility} have placeholder SCAC codes.`,
        confidence: 'high',
      });
      recs.push(`Enforce SCAC validation at ${facility} to reduce UNKNOWN/XXXX records.`);
    }

    // Missing driver contact findings
    if (missingDriverRate !== null && missingDriverRate >= 30) {
      findings.push({
        level: 'yellow',
        text: `${missingDriverRate}% of live loads at ${facility} are missing driver contact information.`,
        confidence: 'high',
      });
      recs.push(`Confirm driver contact capture at ${facility} and train staff to populate contact fields.`);
    }

    return {
      report: 'current_inventory',
      facility,
      meta,
      dataQuality: {
        score: dq,
        label: dq >= 80 ? 'High' : dq >= 50 ? 'Medium' : 'Low',
        color: dq >= 80 ? 'green' : dq >= 50 ? 'yellow' : 'red',
        totalRows: bucket.totalRows,
      },
      metrics: {
        total_trailers: trailers,
        placeholder_scac_pct: placeholderRate,
        outbound_vs_inbound_ratio: outboundInboundRatio !== null ? Math.round(outboundInboundRatio * 100) / 100 : null,
        live_load_missing_driver_contact_pct: missingDriverRate,
      },
      charts,
      findings,
      recommendations: recs,
      roi: null, // current inventory doesn't drive ROI directly
    };
  }
}

class DetentionHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);

    // Actual detention event counters (based on departure vs thresholds)
    this.detention = 0;          // Departed after detention threshold
    this.prevented = 0;          // Departed after pre-detention but before detention
    this.preDetention = 0;       // Approached pre-detention (includes prevented)

    // Tracking for non-events (data quality and status tracking)
    this.stillInYard = 0;        // No departure yet - status pending
    this.unknownStatus = 0;      // Missing threshold data
    this.skippedNoArrival = 0;   // Skipped because no arrival_time

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

    // Drilldown data for detention by carrier (only collected when enableDrilldown is true)
    this.detentionByScacDrilldown = new Map(); // carrier -> array of { trailer, timeInYard, trailerType, detentionDate }

    // Detention spend tracking
    this.detentionEventsWithDeparture = 0;
    this.totalDetentionHours = 0;
    this.detentionHoursArray = [];  // Individual detention hours for median calculation

    // Track data source for CSV mode warning
    this.isCSVMode = opts.isCSVMode || false;
  }

  /**
   * Create a per-facility metrics bucket for detention history.
   */
  createFacilityBucket() {
    return {
      totalRows: 0,
      detention: 0,
      prevented: 0,
      preDetention: 0,
      stillInYard: 0,
      unknownStatus: 0,
      skippedNoArrival: 0,
      live: 0,
      drop: 0,
      detentionLive: 0,
      detentionDrop: 0,
      detentionByScac: new CounterMap(),
      detentionEventsWithDeparture: 0,
      totalDetentionHours: 0,
      dailyDetention: new CounterMap(),
      dailyPrevented: new CounterMap(),
    };
  }

  /**
   * Determines the actual detention status based on arrival, departure, and thresholds.
   *
   * IMPORTANT: Detention Date/Time fields represent the SCHEDULED detention threshold
   * (when detention is set to begin), not whether detention has actually occurred.
   * We must compare actual departure time against these thresholds to determine status.
   *
   * @param {Object} params
   * @param {DateTime} params.arrival - When trailer arrived (required)
   * @param {DateTime|null} params.departure - When trailer departed (null if still in yard)
   * @param {DateTime|null} params.preDetentionThreshold - When pre-detention begins
   * @param {DateTime|null} params.detentionThreshold - When detention begins
   *
   * @returns {Object} Status object with:
   *   - type: 'IN_DETENTION' | 'PREVENTED' | 'NO_DETENTION' | 'STILL_IN_YARD' | 'UNKNOWN'
   *   - detentionHours: number | null (hours spent in detention, if applicable)
   *   - reason: string (explanation for debugging/logging)
   */
  determineDetentionStatus({ arrival, departure, preDetentionThreshold, detentionThreshold }) {
    // Case 1: No departure yet - trailer still in yard
    // Don't count in metrics until trailer completes its cycle
    if (!departure) {
      return {
        type: DETENTION_STATUS.STILL_IN_YARD,
        detentionHours: null,
        reason: 'Trailer has not departed yet'
      };
    }

    // Case 2: Both thresholds are missing - cannot determine detention
    if (!preDetentionThreshold && !detentionThreshold) {
      return {
        type: DETENTION_STATUS.UNKNOWN,
        detentionHours: null,
        reason: 'No detention thresholds configured for this trailer'
      };
    }

    // Case 3: Departed after detention threshold = IN_DETENTION
    // This is actual detention - trailer left after the detention threshold time
    if (detentionThreshold && departure > detentionThreshold) {
      const hours = departure.diff(detentionThreshold, 'hours').hours;
      return {
        type: DETENTION_STATUS.IN_DETENTION,
        detentionHours: Number.isFinite(hours) && hours > 0 ? hours : null,
        reason: `Departed after detention threshold`
      };
    }

    // Case 4: Departed after pre-detention but before detention = PREVENTED
    // This means an intervention worked - trailer left after pre-detention warning
    // but before actual detention began
    if (preDetentionThreshold && departure > preDetentionThreshold) {
      return {
        type: DETENTION_STATUS.PREVENTED,
        detentionHours: null,
        reason: `Departed after pre-detention but before detention`
      };
    }

    // Case 5: Departed before any thresholds = NO_DETENTION
    // Trailer left before even approaching detention - no issue
    return {
      type: DETENTION_STATUS.NO_DETENTION,
      detentionHours: null,
      reason: `Departed before any detention thresholds`
    };
  }

  ingest({ row, flags }) {
    this.totalRows++;

    // Track facility for multi-facility support
    const facility = row._facility || flags?.facility || '';
    const facBucket = this.trackFacility(facility, 'detention_history');
    if (facBucket) {
      facBucket.totalRows++;
    }

    // CRITICAL: Skip rows without arrival_time
    // A trailer cannot be in detention if it never checked in.
    // Arrival date/time is required to determine actual detention status.
    if (isNil(row.arrival_time)) {
      this.skippedNoArrival++;
      if (facBucket) facBucket.skippedNoArrival++;
      return;
    }

    // Parse all required timestamps
    const arrival = parseTimestamp(row.arrival_time, {
      timezone: this.timezone,
      treatAsLocal: true,
      onFail: () => { this.parseFails++; }
    });

    const preDetentionThreshold = parseTimestamp(row.pre_detention_start_time, {
      timezone: this.timezone,
      assumeUTC: true,
      onFail: () => { this.parseFails++; }
    });

    const detentionThreshold = parseTimestamp(row.detention_start_time, {
      timezone: this.timezone,
      assumeUTC: true,
      onFail: () => { this.parseFails++; }
    });

    // Try to find departure datetime (may be null if still in yard)
    const departureRaw = firstPresent(row, [
      'departure_datetime',
      'depart_datetime',
      'yard_out_time',
      'left_yard_time',
      'checkout_time',
      'actual_departure_time'
    ]);

    let departure = null;
    if (departureRaw) {
      departure = parseTimestamp(departureRaw, {
        timezone: this.timezone,
        treatAsLocal: true
      });
    }

    // Validate arrival parsed successfully
    if (!arrival) {
      this.parseFails++;
      return;
    }

    this.parseOk++;
    this.trackDate(arrival);

    // Determine ACTUAL detention status by comparing timestamps.
    // This is the key fix: we compare actual departure time against scheduled thresholds
    // to determine what actually happened (not just whether thresholds were set).
    const status = this.determineDetentionStatus({
      arrival,
      departure,
      preDetentionThreshold,
      detentionThreshold
    });

    // Update counters based on ACTUAL status (not just presence of threshold fields)
    switch(status.type) {
      case DETENTION_STATUS.IN_DETENTION:
        this.detention++;
        if (facBucket) facBucket.detention++;
        break;

      case DETENTION_STATUS.PREVENTED:
        this.prevented++;
        this.preDetention++;
        if (facBucket) {
          facBucket.prevented++;
          facBucket.preDetention++;
        }
        break;

      case DETENTION_STATUS.NO_DETENTION:
        // Departed before thresholds - nothing to count
        break;

      case DETENTION_STATUS.STILL_IN_YARD:
        // No departure yet - don't count in metrics
        this.stillInYard++;
        if (facBucket) facBucket.stillInYard++;
        break;

      case DETENTION_STATUS.UNKNOWN:
        // Missing threshold data
        this.unknownStatus++;
        if (facBucket) facBucket.unknownStatus++;
        break;
    }

    // Track live/drop for ALL rows (general coverage)
    const live = normalizeBoolish(row.live_load) ?? (row.live_load == 1);
    if (live === true) {
      this.live++;
      if (facBucket) facBucket.live++;
    } else if (live === false) {
      this.drop++;
      if (facBucket) facBucket.drop++;
    }

    // Only track events for ACTUAL detention or prevention (not potential)
    const isCountableEvent =
      status.type === DETENTION_STATUS.IN_DETENTION ||
      status.type === DETENTION_STATUS.PREVENTED;

    if (isCountableEvent) {
      // Use the appropriate threshold timestamp for time series grouping
      const eventDt = status.type === DETENTION_STATUS.IN_DETENTION
        ? detentionThreshold
        : preDetentionThreshold;

      // Update time series
      const mk = monthKey(eventDt, this.timezone);
      const wk = weekKey(eventDt, this.timezone);
      const dk = dayKey(eventDt, this.timezone);

      if (status.type === DETENTION_STATUS.IN_DETENTION) {
        this.monthlyDetention.inc(mk);
        this.weeklyDetention.inc(wk);
        this.dailyDetention.inc(dk);
        if (facBucket) facBucket.dailyDetention.inc(dk);
      } else if (status.type === DETENTION_STATUS.PREVENTED) {
        this.monthlyPrevented.inc(mk);
        this.weeklyPrevented.inc(wk);
        this.dailyPrevented.inc(dk);
        if (facBucket) facBucket.dailyPrevented.inc(dk);
      }

      // Track live/drop for detention events only (for pie chart)
      if (live === true) {
        this.detentionLive++;
        if (facBucket) facBucket.detentionLive++;
      } else if (live === false) {
        this.detentionDrop++;
        if (facBucket) facBucket.detentionDrop++;
      }

      // Track SCAC for detention events only (for bar chart)
      const scac = row.scac ?? row.carrier_scac ?? row.scac_code;
      if (!isNil(scac)) {
        this.detentionByScac.inc(scac);
        if (facBucket) facBucket.detentionByScac.inc(scac);

        // Collect drilldown data
        if (this.enableDrilldown) {
          const trailer = safeStr(row.trailer_number || row.trailer_id || row.equipment_number || '');
          const detentionDate = eventDt?.toFormat('yyyy-MM-dd HH:mm') || '';

          // Calculate time in yard (arrival to departure)
          let timeInYard = '';
          if (departure && arrival) {
            const hours = departure.diff(arrival, 'hours').hours;
            if (Number.isFinite(hours) && hours > 0) {
              timeInYard = Math.round(hours * 10) / 10;
            }
          }

          // Get trailer type (Container, Trailer, etc.)
          const trailerType = safeStr(row.trailer_type || '');

          if (!this.detentionByScacDrilldown.has(scac)) {
            this.detentionByScacDrilldown.set(scac, []);
          }
          this.detentionByScacDrilldown.get(scac).push({
            trailer,
            timeInYard,
            trailerType,
            detentionDate
          });
        }
      }

      // Calculate detention spend (for IN_DETENTION only)
      if (status.type === DETENTION_STATUS.IN_DETENTION && status.detentionHours !== null) {
        this.detentionEventsWithDeparture++;
        this.totalDetentionHours += status.detentionHours;
        this.detentionHoursArray.push(status.detentionHours);  // Track for median calculation
        if (facBucket) {
          facBucket.detentionEventsWithDeparture++;
          facBucket.totalDetentionHours += status.detentionHours;
        }
      }
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    // Data quality findings
    const dataQualityFindings = [];

    // Report skipped rows due to missing arrival data
    if (this.skippedNoArrival > 0) {
      const pct = Math.round((this.skippedNoArrival / this.totalRows) * 100);
      findings.push({
        level: 'yellow',
        text: `${pct}% of rows (${this.skippedNoArrival}) are missing arrival date/time and were excluded from detention analysis.`,
        confidence: 'high',
        confidenceReason: 'Arrival date/time is required to determine actual detention status.'
      });
      recs.push('Ensure arrival date/time fields are populated in YMS for accurate detention tracking.');
    }

    // Report trailers still in yard (for informational purposes)
    if (this.stillInYard > 0) {
      findings.push({
        level: 'info',
        text: `${this.stillInYard} trailers are still in the yard with detention rules triggered (no departure recorded yet).`,
        confidence: 'high',
        confidenceReason: 'These trailers have not completed their cycle yet, so detention status is pending.'
      });
    }

    // Update "no detention events" message to account for actual vs potential
    if (this.detention === 0 && this.prevented === 0) {
      if (this.stillInYard > 0) {
        // Some trailers have potential detention but none have completed
        findings.push({
          level: 'yellow',
          text: 'No completed detention events found in this data. Detention rules are triggered but all trailers are still in the yard.',
          confidence: 'high',
          confidenceReason: 'Detention thresholds are configured, but no trailers have departed yet to determine actual status.'
        });
      } else {
        // No detention events at all
        findings.push({
          level: 'yellow',
          text: 'No detention events found in this data. Detention rules may not be configured in YMS.',
          confidence: 'high',
          confidenceReason: 'No detention or pre-detention signals detected in the uploaded data.'
        });
        recs.push('Verify detention rules are configured in YMS. If detention tracking is not needed, this finding can be ignored.');
      }
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

    // QBR Analysis: Overall trend for detention events
    const detentionOverallTrend = computeOverallTrend(
      { monthly: this.monthlyDetention, weekly: this.weeklyDetention, daily: this.dailyDetention },
      'Detention events'
    );
    if (detentionOverallTrend && detentionOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(detentionOverallTrend, {
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Period-over-period comparison for detention
    const detentionPeriodComparison = computePeriodComparison(
      { monthly: this.monthlyDetention, weekly: this.weeklyDetention, daily: this.dailyDetention },
      'Detention events',
      { significantChangePct: 20 }
    );

    // Also check prevented detention for the same period to detect offsetting
    const preventedPeriodComparison = computePeriodComparison(
      { monthly: this.monthlyPrevented, weekly: this.weeklyPrevented, daily: this.dailyPrevented },
      'Prevented detention',
      { significantChangePct: 20 }
    );

    if (detentionPeriodComparison?.isSignificant) {
      // Check if prevention is offsetting detention increases
      if (detentionPeriodComparison.direction === 'increased' &&
          preventedPeriodComparison?.isSignificant &&
          preventedPeriodComparison.direction === 'increased') {

        const detentionIncreaseRate = Math.abs(detentionPeriodComparison.changePct || 0);
        const preventionIncreaseRate = Math.abs(preventedPeriodComparison.changePct || 0);

        // Prevention is keeping pace (increased by at least 80% of detention increase rate)
        if (preventionIncreaseRate >= detentionIncreaseRate * 0.8) {
          findings.push({
            level: 'green',
            text: `Detention events increased by ${detentionIncreaseRate}%, but prevention efforts also increased by ${preventionIncreaseRate}%, effectively mitigating the rise.`,
            confidence: detentionPeriodComparison.confidence || 'high',
            confidenceReason: generateConfidenceReason(detentionPeriodComparison.confidence || 'high', dqFactors)
          });
          recs.push('Detention prevention is scaling effectively with volume. Continue and document current prevention strategies.');
        } else {
          // Prevention increased but not enough to offset
          const finding = formatPeriodComparisonFinding(detentionPeriodComparison, {
            increaseLevel: 'yellow',
            decreaseLevel: 'green',
            dataQualityFactors: dqFactors
          });
          if (finding) {
            finding.text += ` Prevention also increased by ${preventionIncreaseRate}%, but not enough to fully offset.`;
            findings.push(finding);
          }
          recs.push('Detention events increased faster than prevention - review pre-detention alert workflows and early departure processes.');
        }
      } else {
        // Standard finding when no offsetting detected
        const finding = formatPeriodComparisonFinding(detentionPeriodComparison, {
          increaseLevel: 'yellow',
          decreaseLevel: 'green',
          dataQualityFactors: dqFactors
        });
        if (finding) findings.push(finding);

        if (detentionPeriodComparison.direction === 'increased') {
          recs.push('Detention events increased over the period - investigate carrier performance trends and dock scheduling changes.');
        }
      }
    }

    // QBR Analysis: Peak/low periods for detention (automatically detects weekend patterns)
    const detentionPeakLow = findPeakAndLowPeriods(
      { monthly: this.monthlyDetention, weekly: this.weeklyDetention, daily: this.dailyDetention },
      'Detention events',
      { higherIsBetter: false } // Lower detention is better
    );
    if (detentionPeakLow) {
      const finding = formatPeakLowFinding(detentionPeakLow, {
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);

      // Add recommendation if weekend pattern detected
      if (detentionPeakLow.weekendPattern) {
        if (detentionPeakLow.weekendPattern.type === 'worst') {
          recs.push('Weekend detention events are consistently higher - review weekend dock scheduling and carrier arrival patterns.');
        }
      }
    }

    // Operational context: Recent state (day/week/month-over-month)
    const detentionRecentState = computeTrendAnalysis(
      { monthly: this.monthlyDetention, weekly: this.weeklyDetention, daily: this.dailyDetention },
      'Detention events (recent)',
      { significantChangePct: 20 }
    );

    const preventedRecentState = computeTrendAnalysis(
      { monthly: this.monthlyPrevented, weekly: this.weeklyPrevented, daily: this.dailyPrevented },
      'Prevented detention (recent)',
      { significantChangePct: 20 }
    );

    if (detentionRecentState?.isSignificant) {
      // Check if recent prevention trends are offsetting detention trends
      if (detentionRecentState.direction === 'increased' &&
          preventedRecentState?.isSignificant &&
          preventedRecentState.direction === 'increased') {

        const detentionRate = Math.abs(detentionRecentState.changePct || 0);
        const preventionRate = Math.abs(preventedRecentState.changePct || 0);

        if (preventionRate >= detentionRate * 0.8) {
          findings.push({
            level: 'green',
            text: `Recent state: Detention up ${detentionRate}%, but prevention up ${preventionRate}% - mitigating impact.`,
            confidence: detentionRecentState.confidence || 'high',
            confidenceReason: generateConfidenceReason(detentionRecentState.confidence || 'high', dqFactors)
          });
        } else {
          const finding = formatTrendFinding(detentionRecentState, {
            increaseLevel: 'yellow',
            decreaseLevel: 'green',
            dataQualityFactors: dqFactors
          });
          if (finding) {
            finding.text = `Recent state: ${finding.text} (prevention up ${preventionRate}%, but not keeping pace)`;
            findings.push(finding);
          }
        }
      } else {
        const finding = formatTrendFinding(detentionRecentState, {
          increaseLevel: 'yellow',
          decreaseLevel: 'green',
          dataQualityFactors: dqFactors
        });
        if (finding) {
          finding.text = `Recent state: ${finding.text}`;
          findings.push(finding);
        }
      }
    }

    // QBR Analysis: Prevented detention trends
    const preventedOverallTrend = computeOverallTrend(
      { monthly: this.monthlyPrevented, weekly: this.weeklyPrevented, daily: this.dailyPrevented },
      'Prevented detention'
    );
    if (preventedOverallTrend && preventedOverallTrend.trendDirection !== 'stable' && preventedOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(preventedOverallTrend, {
        increaseLevel: 'green',   // More prevented is good
        decreaseLevel: 'yellow',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Prevention success rate
    // Calculate overall prevention effectiveness
    if (this.detention > 0 || this.prevented > 0) {
      const totalDetentionAttempts = this.detention + this.prevented;
      const preventionRate = Math.round((this.prevented / totalDetentionAttempts) * 100);

      // Strong prevention rate (60%+) with sufficient sample size
      if (preventionRate >= 60 && totalDetentionAttempts >= 10) {
        findings.push({
          level: 'green',
          text: `Strong detention prevention: ${preventionRate}% prevention rate (${this.prevented} prevented out of ${totalDetentionAttempts} potential detentions).`,
          confidence: 'high',
          confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: totalDetentionAttempts })
        });
        recs.push('Detention prevention efforts are effective. Document and replicate successful workflows.');
      }
      // Low prevention rate (<30%) - needs attention
      else if (preventionRate < 30 && totalDetentionAttempts >= 10) {
        findings.push({
          level: 'yellow',
          text: `Low detention prevention rate: ${preventionRate}% (only ${this.prevented} prevented out of ${totalDetentionAttempts} potential detentions).`,
          confidence: 'high',
          confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: totalDetentionAttempts })
        });
        recs.push('Review pre-detention alerts and workflows to improve early departure rates.');
      }
      // Moderate prevention rate (30-59%) - show as info
      else if (totalDetentionAttempts >= 10) {
        findings.push({
          level: 'info',
          text: `Detention prevention rate: ${preventionRate}% (${this.prevented} prevented, ${this.detention} entered detention).`,
          confidence: 'high',
          confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: totalDetentionAttempts })
        });
      }
    }

    // Summary finding when no trends available but data exists
    if (!detentionRecentState && !preventedOverallTrend && (this.detention > 0 || this.preDetention > 0)) {
      const totalAttempts = this.detention + this.prevented;
      const preventionRate = totalAttempts > 0 ? Math.round((this.prevented / totalAttempts) * 100) : 0;

      let summaryText = `Detention: ${this.detention} events, Prevented: ${this.prevented}`;
      if (totalAttempts >= 5) {
        summaryText += ` (${preventionRate}% prevention rate)`;
      }
      summaryText += '.';

      findings.push({
        level: preventionRate >= 50 ? 'green' : 'info',
        text: summaryText,
        confidence: 'high',
        confidenceReason: generateConfidenceReason('high', { ...dqFactors, sampleSize: totalAttempts })
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

    // Aggregate facility-level issues for "All Facilities" view
    if (this.byFacility.size > 1) {
      const facilityIssues = {
        highDetention: [],
        lowPreventionRate: []
      };

      for (const [facilityName, bucket] of this.byFacility) {
        // Calculate facility-level metrics
        const detentionCount = bucket.detention || 0;
        const preventedCount = bucket.prevented || 0;
        const totalEvents = detentionCount + preventedCount;
        const preventionRate = totalEvents > 0 ? Math.round((preventedCount / totalEvents) * 100) : null;
        const avgDetentionHours = bucket.detentionEventsWithDeparture > 0
          ? Math.round((bucket.totalDetentionHours / bucket.detentionEventsWithDeparture) * 10) / 10
          : null;

        // Flag facilities with high detention counts
        if (detentionCount > 10) {
          facilityIssues.highDetention.push({ name: facilityName, value: detentionCount, avgHours: avgDetentionHours });
        }
        // Flag facilities with low prevention rates (if they have events)
        if (preventionRate !== null && preventionRate < 30 && totalEvents >= 5) {
          facilityIssues.lowPreventionRate.push({ name: facilityName, value: preventionRate, detentions: detentionCount });
        }
      }

      // Add aggregated findings
      if (facilityIssues.highDetention.length > 0) {
        const worst = facilityIssues.highDetention.sort((a, b) => b.value - a.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: worst[0].value > 50 ? 'red' : 'yellow',
          text: `${facilityIssues.highDetention.length} of ${this.byFacility.size} facilities have elevated detention events: ${names}. Highest: ${worst[0].name} with ${worst[0].value} events${worst[0].avgHours ? ` (avg ${worst[0].avgHours} hrs)` : ''}.`,
          confidence: 'high',
          confidenceReason: 'Based on detention event counts per facility.'
        });
        recs.push(`Investigate detention drivers at high-detention facilities: ${names}.`);
      }

      if (facilityIssues.lowPreventionRate.length > 0) {
        const worst = facilityIssues.lowPreventionRate.sort((a, b) => a.value - b.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: 'yellow',
          text: `${facilityIssues.lowPreventionRate.length} of ${this.byFacility.size} facilities have low detention prevention rates (<30%): ${names}. Lowest: ${worst[0].name} at ${worst[0].value}%.`,
          confidence: 'medium',
          confidenceReason: 'Based on prevention rate per facility.'
        });
        recs.push(`Review detention prevention workflows at: ${names}.`);
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
          },
          drilldown: this.enableDrilldown ? {
            columns: ['trailer', 'timeInYard', 'trailerType', 'detentionDate'],
            columnLabels: ['Trailer #', 'Time in Yard (Hours)', 'Trailer Type', 'Detention Date'],
            byLabel: Object.fromEntries(
              this.detentionByScac.top(10).map(x => [x.key, this.detentionByScacDrilldown.get(x.key) || []])
            )
          } : null
        }] : [])
      ],
      findings,
      recommendations: recs,
      roi: computeDetentionROIIfEnabled({
        meta,
        metrics: {
          prevented: this.prevented,
          detentionHoursArray: this.detentionHoursArray,
          detentionEventsWithDeparture: this.detentionEventsWithDeparture,
          totalDetentionHours: this.totalDetentionHours,
          monthlyPrevented: this.monthlyPrevented,
          weeklyPrevented: this.weeklyPrevented,
          dailyPrevented: this.dailyPrevented,
        },
        assumptions: meta.assumptions
      }),
      // Additional ROI for detention spend
      detentionSpend: computeDetentionSpendIfEnabled({
        metrics: {
          detentionEvents: this.detentionEventsWithDeparture,
          totalDetentionHours: this.totalDetentionHours,
          actualDetentionCount: this.detention,  // True count of detention events (for PM note)
          detentionHoursArray: this.detentionHoursArray,  // Individual hours for outlier detection
        },
        assumptions: meta.assumptions,
      }),
    };
  }

  /**
   * Finalize results for a specific facility.
   * Returns a simplified result with key metrics for tabbed display.
   */
  finalizeFacility(facility, meta) {
    // Normalize facility name to ensure consistent lookup with how buckets are stored
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;

    const bucket = this.byFacility.get(normalized);
    if (!bucket) return null;

    const totalEvents = bucket.preDetention + bucket.detention;
    const preventionRate = totalEvents > 0 ? Math.round((bucket.prevented / totalEvents) * 100) : null;

    // Simplified data quality
    const dq = bucket.totalRows > 0 ? 75 : 0;

    // Build simple time series chart if data available
    const charts = [];
    if (bucket.dailyDetention && bucket.dailyDetention.map.size > 0) {
      const labels = Array.from(bucket.dailyDetention.map.keys()).sort();
      const detentionData = labels.map(l => bucket.dailyDetention.map.get(l) || 0);
      const preventedData = labels.map(l => bucket.dailyPrevented?.map.get(l) || 0);

      charts.push({
        id: 'detention_vs_prevented_daily',
        title: 'Detention vs Prevented (Daily)',
        kind: 'line',
        data: {
          labels,
          datasets: [
            { label: 'Detention', data: detentionData },
            { label: 'Prevented', data: preventedData },
          ]
        }
      });
    }

    // Carrier breakdown if available
    if (bucket.detentionByScac && bucket.detentionByScac.map.size > 0) {
      const top10 = bucket.detentionByScac.top(10);
      charts.push({
        id: 'detention_by_carrier',
        title: 'Detention by Carrier (Top 10)',
        kind: 'bar',
        data: {
          labels: top10.map(x => x.key),
          datasets: [{
            label: 'Detention events',
            data: top10.map(x => x.value)
          }]
        }
      });
    }

    // Generate findings and recommendations for this facility
    const findings = [];
    const recs = [];

    // Prevention rate findings
    if (preventionRate !== null) {
      if (preventionRate >= 80) {
        findings.push({
          level: 'green',
          text: `${facility} has excellent detention prevention rate of ${preventionRate}%.`,
          confidence: 'high',
        });
      } else if (preventionRate >= 50) {
        findings.push({
          level: 'yellow',
          text: `${facility} has moderate detention prevention rate of ${preventionRate}%.`,
          confidence: 'high',
        });
        recs.push(`Review carrier scheduling at ${facility} to improve detention prevention.`);
      } else if (preventionRate < 50 && totalEvents > 0) {
        findings.push({
          level: 'red',
          text: `${facility} has low detention prevention rate of ${preventionRate}%.`,
          confidence: 'high',
        });
        recs.push(`Investigate carrier compliance and scheduling practices at ${facility}.`);
      }
    }

    // Detention event findings
    if (bucket.detention > 10) {
      findings.push({
        level: 'yellow',
        text: `${facility} has ${bucket.detention} detention events that may incur carrier charges.`,
        confidence: 'high',
      });
    }

    return {
      report: 'detention_history',
      facility,
      meta,
      dataQuality: {
        score: dq,
        label: dq >= 80 ? 'High' : dq >= 50 ? 'Medium' : 'Low',
        color: dq >= 80 ? 'green' : dq >= 50 ? 'yellow' : 'red',
        totalRows: bucket.totalRows,
      },
      metrics: {
        pre_detention_count: bucket.preDetention,
        detention_count: bucket.detention,
        prevented_detention_count: bucket.prevented,
        prevention_rate: preventionRate,
        live_load_count: bucket.detentionLive,
        drop_load_count: bucket.detentionDrop,
      },
      charts,
      findings,
      recommendations: recs,
      roi: computeDetentionROIIfEnabled({
        meta,
        metrics: {
          prevented: bucket.prevented,
          // Use overall detention hours for median calculation (not facility-specific)
          detentionHoursArray: this.detentionHoursArray,
          detentionEventsWithDeparture: this.detentionEventsWithDeparture,
          totalDetentionHours: this.totalDetentionHours,
          // Use facility-specific prevention time series
          monthlyPrevented: bucket.monthlyPrevented,
          weeklyPrevented: bucket.weeklyPrevented,
          dailyPrevented: bucket.dailyPrevented,
        },
        assumptions: meta.assumptions
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

    // Drilldown data for outlier days (only collected when enableDrilldown is true)
    // Stores all records by day so we can show details when clicking outlier points
    this.recordsByDay = new Map(); // day -> array of { trailer, dwellMins, checkIn, checkOut }
  }

  /**
   * Create a per-facility metrics bucket for dock door history.
   */
  createFacilityBucket() {
    return {
      totalRows: 0,
      dwellCoverage: { ok: 0, total: 0 },
      processCoverage: { ok: 0, total: 0 },
      dwellByDay: new Map(),
      processByDay: new Map(),
      uniqueDoors: new Set(),
      totalTurns: 0,
      daysWithData: new Set(),
      turnsByDay: new CounterMap(),

      // Add quantile estimators for per-facility metrics
      dwellQuantile: new P2Quantile(0.5),
      processQuantile: new P2Quantile(0.5),

      // Add structure for turns per door per day calculation
      turnsByDoorByDay: new Map(), // Map<door, Map<day, count>>
    };
  }

  getEstimators(map, key) {
    if (!map.has(key)) {
      map.set(key, { median: new P2Quantile(0.5), p90: new P2Quantile(0.9) });
    }
    return map.get(key);
  }

  ingest({ row, flags }) {
    this.totalRows++;

    // Track facility for multi-facility support
    const facility = row._facility || flags?.facility || '';
    const facBucket = this.trackFacility(facility, 'dockdoor_history');
    if (facBucket) {
      facBucket.totalRows++;
    }

    // Get event type to properly handle paired events
    const event = safeStr(firstPresent(row, ['event', 'event_type', 'event_name']));

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

    // Dwell Coverage: Count every row, mark "ok" if it has at least dwell start OR end
    // This way "Dwell Started" and "Dwell Ended" events both count as good data quality
    this.dwellCoverage.total++;
    if (facBucket) facBucket.dwellCoverage.total++;
    if (dwellStart || dwellEnd) {
      this.dwellCoverage.ok++;
      if (facBucket) facBucket.dwellCoverage.ok++;
    }

    // Calculate dwell time metrics when we have both start and end
    if (dwellStart && dwellEnd) {
      this.parseOk++;
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

        // Add dwell quantile for per-facility metrics
        if (facBucket && facBucket.dwellQuantile) {
          facBucket.dwellQuantile.add(mins);
          // Also track time series for per-facility charts
          this.getEstimators(facBucket.dwellByDay, dk).median.add(mins);
          facBucket.daysWithData.add(dk);
        }

        // Collect drilldown data if enabled (store by day for outlier lookup)
        if (this.enableDrilldown) {
          const trailer = safeStr(row.trailer_number || row.trailer_id || row.equipment_number || '');
          const checkIn = dwellStart.toFormat('yyyy-MM-dd HH:mm');
          const checkOut = dwellEnd.toFormat('yyyy-MM-dd HH:mm');
          const dwellMins = Math.round(mins);
          if (!this.recordsByDay.has(dk)) {
            this.recordsByDay.set(dk, []);
          }
          this.recordsByDay.get(dk).push({ trailer, dwellMins, checkIn, checkOut });
        }
      }
    }

    // Process Coverage: Measure feature adoption
    // Only count "Dwell Ended" events (complete dock door visits) toward the total
    // Mark "ok" if the visit also has process data (feature was used)
    const isDwellEnded = /dwell\s+ended/i.test(event);
    if (isDwellEnded) {
      this.processCoverage.total++;
      if (facBucket) facBucket.processCoverage.total++;
      if (procStartRaw || procEndRaw) {
        this.processCoverage.ok++;
        if (facBucket) facBucket.processCoverage.ok++;
      }
    }

    // Calculate process time metrics when we have both start and end
    if (procStart && procEnd) {
      this.parseOk++;
      const mins = procEnd.diff(procStart, 'minutes').minutes;
      if (Number.isFinite(mins) && mins >= 0) {
        // Track at multiple granularities for trend analysis fallback
        const mk = monthKey(procStart, this.timezone);
        const wk = weekKey(procStart, this.timezone);
        const dk = dayKey(procStart, this.timezone);
        this.getEstimators(this.processByMonth, mk).median.add(mins);
        this.getEstimators(this.processByWeek, wk).median.add(mins);
        this.getEstimators(this.processByDay, dk).median.add(mins);

        // Add process quantile for per-facility metrics
        if (facBucket && facBucket.processQuantile) {
          facBucket.processQuantile.add(mins);
          // Also track time series for per-facility charts
          this.getEstimators(facBucket.processByDay, dk).median.add(mins);
        }
      }
    }

    // Leaderboards (only if sample size sufficient)
    const processedBy = safeStr(firstPresent(row, ['processed_by', 'processed_by_name', 'processed_by_user']));
    if (processedBy) this.processedBy.inc(processedBy);

    const requestedBy = safeStr(firstPresent(row, ['move_requested_by', 'requested_by', 'move_requested_by_name']));
    if (requestedBy) {
      this.moveRequestedBy.inc(requestedBy);
      this.rowsWithRequester++;
    }

    // Track dock door turns for throughput ROI
    const rawDoor = safeStr(firstPresent(row, ['door', 'door_name', 'dock_door', 'dock_door_name', 'door_id', 'location', 'location_name']));
    const door = normalizeDoorName(rawDoor);
    const eventDt = dwellStart || procStart;
    if (door && eventDt) {
      const dk = dayKey(eventDt, this.timezone);
      const wk = weekKey(eventDt, this.timezone);
      const mk = monthKey(eventDt, this.timezone);

      this.uniqueDoors.add(door);
      this.daysWithData.add(dk);
      this.totalTurns++;

      // Track per-facility throughput
      if (facBucket) {
        facBucket.uniqueDoors.add(door);
        facBucket.daysWithData.add(dk);
        facBucket.totalTurns++;
        facBucket.turnsByDay.inc(dk);

        // Track turnsByDoorByDay for avg calculation
        if (!facBucket.turnsByDoorByDay.has(door)) {
          facBucket.turnsByDoorByDay.set(door, new Map());
        }
        const facDoorDays = facBucket.turnsByDoorByDay.get(door);
        facDoorDays.set(dk, (facDoorDays.get(dk) || 0) + 1);
      }

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
      dataQualityFindings.push({ level: 'yellow', text: `Dwell time data quality: ${dwellCoveragePct}% of records have dwell timestamps (start or end). Missing timestamps may indicate integration issues.` });
      recs.push('Confirm dwell start/end timestamps are being recorded consistently (workflow + integrations).');
    }
    if (processCoveragePct < 60) {
      dataQualityFindings.push({ level: 'yellow', text: `YMS processing feature adoption: Only ${processCoveragePct}% of dock door visits used the loading/unloading feature. Low adoption may indicate training gaps or workflow issues.` });
      recs.push('Increase adoption of the YMS dock door loading/unloading feature to improve visibility into process times.');
    }

    // QBR Analysis: Overall trend for dwell time
    const dwellOverallTrend = computeOverallTrend(
      { monthly: this.dwellByMonth, weekly: this.dwellByWeek, daily: this.dwellByDay },
      'Median dwell time'
    );
    if (dwellOverallTrend && dwellOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(dwellOverallTrend, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Period-over-period comparison for dwell time
    const dwellPeriodComparison = computePeriodComparison(
      { monthly: this.dwellByMonth, weekly: this.dwellByWeek, daily: this.dwellByDay },
      'Median dwell time',
      { significantChangePct: 15 }
    );
    if (dwellPeriodComparison?.isSignificant) {
      const finding = formatPeriodComparisonFinding(dwellPeriodComparison, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Peak/low periods for dwell time (automatically detects weekend patterns)
    const dwellPeakLow = findPeakAndLowPeriods(
      { monthly: this.dwellByMonth, weekly: this.dwellByWeek, daily: this.dwellByDay },
      'Median dwell time',
      { higherIsBetter: false } // Lower dwell is better
    );
    if (dwellPeakLow) {
      const finding = formatPeakLowFinding(dwellPeakLow, {
        unit: ' min',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);

      // Add recommendation if weekend pattern detected
      if (dwellPeakLow.weekendPattern) {
        if (dwellPeakLow.weekendPattern.type === 'worst') {
          recs.push('Weekend dwell times are consistently higher - review weekend staffing and dock availability.');
        }
      }
    }

    // Operational context: Recent state for dwell time
    const dwellRecentState = computeTrendAnalysis(
      { monthly: this.dwellByMonth, weekly: this.dwellByWeek, daily: this.dwellByDay },
      'Median dwell time (recent)',
      { significantChangePct: 15 }
    );
    if (dwellRecentState?.isSignificant) {
      const finding = formatTrendFinding(dwellRecentState, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) {
        finding.text = `Recent state: ${finding.text}`;
        findings.push(finding);
      }
    }

    // QBR Analysis: Overall trend for process time
    const processOverallTrend = computeOverallTrend(
      { monthly: this.processByMonth, weekly: this.processByWeek, daily: this.processByDay },
      'Median process time'
    );
    if (processOverallTrend && processOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(processOverallTrend, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Period-over-period comparison for process time
    const processPeriodComparison = computePeriodComparison(
      { monthly: this.processByMonth, weekly: this.processByWeek, daily: this.processByDay },
      'Median process time',
      { significantChangePct: 15 }
    );
    if (processPeriodComparison?.isSignificant) {
      const finding = formatPeriodComparisonFinding(processPeriodComparison, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Peak/low periods for process time
    const processPeakLow = findPeakAndLowPeriods(
      { monthly: this.processByMonth, weekly: this.processByWeek, daily: this.processByDay },
      'Median process time',
      { higherIsBetter: false } // Lower process time is better
    );
    if (processPeakLow) {
      const finding = formatPeakLowFinding(processPeakLow, {
        unit: ' min',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // Operational context: Recent state for process time
    const processRecentState = computeTrendAnalysis(
      { monthly: this.processByMonth, weekly: this.processByWeek, daily: this.processByDay },
      'Median process time (recent)',
      { significantChangePct: 15 }
    );
    if (processRecentState?.isSignificant) {
      const finding = formatTrendFinding(processRecentState, {
        unit: ' min',
        increaseLevel: 'yellow',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) {
        finding.text = `Recent state: ${finding.text}`;
        findings.push(finding);
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

    // Aggregate facility-level issues for "All Facilities" view
    if (this.byFacility.size > 1) {
      const facilityIssues = {
        highDwell: [],
        lowProcessAdoption: [],
        lowDoorUtilization: []
      };

      for (const [facilityName, bucket] of this.byFacility) {
        const medDwell = bucket.dwellQuantile?.value() ?? null;
        const processCoveragePct = bucket.processCoverage.total > 0
          ? Math.round((bucket.processCoverage.ok / bucket.processCoverage.total) * 100)
          : null;
        const turnsPerDoorPerDay = this.calculateFacilityTurnsPerDoorPerDay(bucket);

        if (medDwell !== null && medDwell > 120) {
          facilityIssues.highDwell.push({ name: facilityName, value: Math.round(medDwell) });
        }
        if (processCoveragePct !== null && processCoveragePct < 70) {
          facilityIssues.lowProcessAdoption.push({ name: facilityName, value: processCoveragePct });
        }
        if (turnsPerDoorPerDay !== null && turnsPerDoorPerDay < 5) {
          facilityIssues.lowDoorUtilization.push({ name: facilityName, value: turnsPerDoorPerDay });
        }
      }

      // Add aggregated findings
      if (facilityIssues.highDwell.length > 0) {
        const worst = facilityIssues.highDwell.sort((a, b) => b.value - a.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: worst[0].value > 180 ? 'red' : 'yellow',
          text: `${facilityIssues.highDwell.length} of ${this.byFacility.size} facilities have high dwell times (>120 min): ${names}. Worst: ${worst[0].name} at ${worst[0].value} min.`,
          confidence: 'high',
          confidenceReason: 'Based on median dwell times per facility.'
        });
        recs.push(`Focus dock optimization efforts on high-dwell facilities: ${names}.`);
      }

      if (facilityIssues.lowProcessAdoption.length > 0) {
        const worst = facilityIssues.lowProcessAdoption.sort((a, b) => a.value - b.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: worst[0].value < 50 ? 'red' : 'yellow',
          text: `${facilityIssues.lowProcessAdoption.length} of ${this.byFacility.size} facilities have low process feature adoption (<70%): ${names}. Lowest: ${worst[0].name} at ${worst[0].value}%.`,
          confidence: 'high',
          confidenceReason: 'Based on process coverage percentage per facility.'
        });
        recs.push(`Prioritize YMS process feature training at: ${names}.`);
      }

      if (facilityIssues.lowDoorUtilization.length > 0) {
        const worst = facilityIssues.lowDoorUtilization.sort((a, b) => a.value - b.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: 'yellow',
          text: `${facilityIssues.lowDoorUtilization.length} of ${this.byFacility.size} facilities have low door utilization (<5 turns/door/day): ${names}.`,
          confidence: 'medium',
          confidenceReason: 'Based on average door turns per facility.'
        });
      }
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
          },
          // Drilldown for outlier points - only for days with outliers
          drilldown: this.enableDrilldown && dwellOutliers.hasOutliers ? {
            columns: ['trailer', 'dwellMins', 'checkIn', 'checkOut'],
            columnLabels: ['Trailer #', 'Dwell (min)', 'Check-In', 'Check-Out'],
            byLabel: Object.fromEntries(
              dwellOutliers.outlierLabels.map(label => [label, this.recordsByDay.get(label) || []])
            ),
            outlierOnly: true // Flag to indicate only outlier points are clickable
          } : null
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

  /**
   * Finalize results for a specific facility.
   * Returns a simplified result with key metrics for tabbed display.
   */
  finalizeFacility(facility, meta) {
    // Normalize facility name to ensure consistent lookup with how buckets are stored
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;

    const bucket = this.byFacility.get(normalized);
    if (!bucket) return null;

    // Calculate key metrics from bucket
    const medDwell = bucket.dwellQuantile?.value() ?? null;
    const medProcess = bucket.processQuantile?.value() ?? null;
    const turnsPerDoorPerDay = this.calculateFacilityTurnsPerDoorPerDay(bucket);
    const processCoveragePct = bucket.processCoverage.total > 0
      ? Math.round((bucket.processCoverage.ok / bucket.processCoverage.total) * 100)
      : null;

    // Simplified data quality
    const dq = bucket.totalRows > 0 ? 75 : 0;

    // Build simple charts
    const charts = [];

    // Dwell/process time series if available
    if (bucket.dwellByDay && bucket.dwellByDay.size > 0) {
      const labels = Array.from(bucket.dwellByDay.keys()).sort();
      const dwellData = labels.map(l => {
        const val = bucket.dwellByDay.get(l);
        return val?.median?.value() ?? 0;
      });
      const processData = labels.map(l => {
        const val = bucket.processByDay?.get(l);
        return val?.median?.value() ?? 0;
      });

      charts.push({
        id: 'dwell_process_daily',
        title: `Median Dwell vs Process Time (Daily) - ${facility}`,
        kind: 'line',
        description: 'Median minutes per day for this facility.',
        data: {
          labels,
          datasets: [
            { label: 'Dwell median (min)', data: dwellData },
            { label: 'Process median (min)', data: processData },
          ]
        }
      });
    }

    // Door turns chart if available
    if (bucket.turnsByDay && bucket.turnsByDay.map && bucket.turnsByDay.map.size > 0) {
      const turnsLabels = Array.from(bucket.turnsByDay.map.keys()).sort();
      const turnsCounts = turnsLabels.map(l => bucket.turnsByDay.map.get(l) || 0);

      charts.push({
        id: 'door_turns_daily',
        title: `Door Turns (Daily) - ${facility}`,
        kind: 'bar',
        description: 'Number of door turns per day.',
        data: {
          labels: turnsLabels,
          datasets: [
            { label: 'Door turns', data: turnsCounts }
          ]
        }
      });
    }

    return {
      report: 'dockdoor_history',
      facility,
      meta,
      dataQuality: {
        score: dq,
        label: dq >= 80 ? 'High' : dq >= 50 ? 'Medium' : 'Low',
        color: dq >= 80 ? 'green' : dq >= 50 ? 'yellow' : 'red',
        totalRows: bucket.totalRows,
      },
      metrics: {
        median_dwell_time_min: medDwell !== null ? Math.round(medDwell) : null,
        median_process_time_min: medProcess !== null ? Math.round(medProcess) : null,
        avg_turns_per_door_per_day: turnsPerDoorPerDay,
        process_adoption_pct: processCoveragePct,
        unique_doors: bucket.uniqueDoors?.size || 0,
        total_turns: bucket.totalTurns || 0,
        // Debug: list all unique door values to diagnose counting issues
        _debug_door_list: bucket.uniqueDoors ? [...bucket.uniqueDoors].sort() : [],
      },
      charts,
      findings: this.generateFacilityFindings(facility, bucket, medDwell, medProcess, processCoveragePct, turnsPerDoorPerDay),
      recommendations: this.generateFacilityRecommendations(facility, bucket, medDwell, medProcess, processCoveragePct, turnsPerDoorPerDay),
      roi: computeDockDoorROIIfEnabled({
        meta,
        metrics: {
          turnsPerDoorPerDay,
          uniqueDoors: bucket.uniqueDoors?.size || 0,
          totalTurns: bucket.totalTurns || 0,
          totalDays: bucket.daysWithData?.size || 0,
        },
        assumptions: meta.assumptions
      }),
    };
  }

  /**
   * Generate findings for a specific facility
   */
  generateFacilityFindings(facility, bucket, medDwell, medProcess, processCoveragePct, turnsPerDoorPerDay) {
    const findings = [];

    // Finding 1: High dwell time
    if (medDwell !== null && medDwell > 120) {
      findings.push({
        level: medDwell > 180 ? 'red' : 'yellow',
        text: `Median dwell time at ${facility} is ${Math.round(medDwell)} minutes, indicating potential dock congestion or slow processing.`,
        confidence: 'medium',
        confidenceReason: `Based on ${bucket.dwellCoverage.ok} door visits with valid dwell times.`,
      });
    }

    // Finding 2: Low process adoption
    if (processCoveragePct !== null && processCoveragePct < 70) {
      findings.push({
        level: processCoveragePct < 50 ? 'red' : 'yellow',
        text: `Process feature adoption at ${facility} is only ${processCoveragePct}%, indicating incomplete YMS workflow usage.`,
        confidence: 'high',
        confidenceReason: `Based on ${bucket.processCoverage.total} door visits.`,
      });
    }

    // Finding 3: Low door utilization
    if (turnsPerDoorPerDay !== null && turnsPerDoorPerDay < 5) {
      findings.push({
        level: 'yellow',
        text: `Average turns per door per day at ${facility} is ${turnsPerDoorPerDay}, suggesting underutilized dock capacity.`,
        confidence: 'medium',
        confidenceReason: `Based on ${bucket.uniqueDoors.size} unique doors across ${bucket.daysWithData.size} days.`,
      });
    }

    // Finding 4: Excellent performance (positive finding)
    if (medDwell !== null && medDwell < 60 && processCoveragePct > 90) {
      findings.push({
        level: 'green',
        text: `${facility} demonstrates excellent dock performance with ${Math.round(medDwell)}-minute median dwell time and ${processCoveragePct}% process adoption.`,
        confidence: 'high',
        confidenceReason: `Based on comprehensive data from ${bucket.totalRows} records.`,
      });
    }

    return findings;
  }

  /**
   * Generate recommendations for a specific facility
   */
  generateFacilityRecommendations(facility, bucket, medDwell, medProcess, processCoveragePct, turnsPerDoorPerDay) {
    const recommendations = [];

    // Recommendation for high dwell time
    if (medDwell !== null && medDwell > 180) {
      recommendations.push(`Investigate dock operations at ${facility} to identify bottlenecks causing excessive dwell times.`);
    }

    // Recommendation for low process adoption
    if (processCoveragePct !== null && processCoveragePct < 70) {
      recommendations.push(`Provide training to drivers at ${facility} on using the YMS process feature for dock door visits.`);
    }

    // Recommendation for low door utilization
    if (turnsPerDoorPerDay !== null && turnsPerDoorPerDay < 5) {
      recommendations.push(`Review dock door allocation at ${facility} to optimize utilization or consider reducing active door count.`);
    }

    return recommendations;
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

    return totalDoorDays > 0 ? Math.round((totalTurnsCount / totalDoorDays) * 100) / 100 : 0;
  }

  // Calculate average turns per door per day for a specific facility bucket
  calculateFacilityTurnsPerDoorPerDay(bucket) {
    if (!bucket.turnsByDoorByDay || bucket.turnsByDoorByDay.size === 0) return 0;
    if (!bucket.uniqueDoors || bucket.uniqueDoors.size === 0) return 0;

    let totalDoorDays = 0;
    let totalTurnsCount = 0;

    for (const [, doorMap] of bucket.turnsByDoorByDay) {
      for (const [, turns] of doorMap) {
        totalDoorDays++;
        totalTurnsCount += turns;
      }
    }

    return totalDoorDays > 0 ? Math.round((totalTurnsCount / totalDoorDays) * 100) / 100 : 0;
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

    // Dispatch efficiency: deadhead (accept→start) vs execution (start→complete)
    this.deadheadMedian = new P2Quantile(0.5);
    this.deadheadP90 = new P2Quantile(0.9);
    this.executionMedian = new P2Quantile(0.5);
    this.executionP90 = new P2Quantile(0.9);
    this.dispatchTotal = 0;

    // Day boundaries: moves per day + approx distinct drivers per day
    this.movesByDay = new CounterMap();
    this.activeDriversByDay = new Map();

    // Track days worked per driver (for accurate per-day averages)
    this.daysWorkedByDriver = new Map(); // driver -> Set of day keys
  }

  /**
   * Create a per-facility metrics bucket for driver history.
   */
  createFacilityBucket() {
    return {
      totalRows: 0,
      movesTotal: 0,
      movesByDriver: new CounterMap(),
      movesByDay: new CounterMap(),
      activeDriversByDay: new Map(),
      complianceOk: 0,
      complianceTotal: 0,
      queueMedian: new P2Quantile(0.5),
      queueP90: new P2Quantile(0.9),
      queueTotal: 0,
      deadheadMedian: new P2Quantile(0.5),
      executionMedian: new P2Quantile(0.5),
      dispatchTotal: 0,
    };
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

  ingest({ row, flags }) {
    this.totalRows++;

    // Track facility for multi-facility support
    const facility = row._facility || flags?.facility || '';
    const facBucket = this.trackFacility(facility, 'driver_history');
    if (facBucket) {
      facBucket.totalRows++;
    }

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
      if (facBucket) facBucket.movesTotal++;
      if (driver) {
        this.movesByDriver.inc(driver);
        if (facBucket) facBucket.movesByDriver.inc(driver);
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
      if (facBucket) facBucket.movesByDay.inc(dy);
      if (driver) {
        this.getDistinct(this.activeDriversByWeek, wk).add(driver);
        this.getDistinct(this.activeDriversByDay, dy).add(driver);
        // Track which days each driver worked (for accurate per-day averages)
        this.getDaysWorked(driver).add(dy);
        // Track per-facility active drivers
        if (facBucket) {
          if (!facBucket.activeDriversByDay.has(dy)) {
            facBucket.activeDriversByDay.set(dy, new ApproxDistinct(2048));
          }
          facBucket.activeDriversByDay.get(dy).add(driver);
        }
      }
    }

    // Compliance rule:
    // % moves where accept/start/complete within <=2 minutes OR elapsed_time_minutes <= 2.
    this.complianceTotal++;
    if (facBucket) facBucket.complianceTotal++;
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
    if (ok) {
      this.complianceOk++;
      if (facBucket) facBucket.complianceOk++;
    }

    // Queue time
    const q = maybeNumber(firstPresent(row, ['time_in_queue_minutes', 'queue_time_minutes', 'time_in_queue']));
    if (Number.isFinite(q) && q >= 0) {
      this.queueMedian.add(q);
      this.queueP90.add(q);
      this.queueTotal++;
      if (facBucket) {
        facBucket.queueMedian.add(q);
        facBucket.queueP90.add(q);
        facBucket.queueTotal++;
      }
    }

    // Dispatch efficiency: deadhead (accept→start) vs execution (start→complete)
    if (accept && start && complete) {
      let deadheadMin = start.diff(accept, 'minutes').minutes;
      let executionMin = complete.diff(start, 'minutes').minutes;

      // Cross-midnight correction: CSV times share a single date, so midnight crossings
      // result in negative diffs. Add 24h if negative but plausible.
      if (deadheadMin < 0 && deadheadMin > -1440) deadheadMin += 1440;
      if (executionMin < 0 && executionMin > -1440) executionMin += 1440;

      // Only count if times are reasonable (>30s, <4 hours)
      const MAX_REASONABLE = 240;
      if (deadheadMin > 0.5 && deadheadMin <= MAX_REASONABLE &&
          executionMin > 0.5 && executionMin <= MAX_REASONABLE) {
        this.deadheadMedian.add(deadheadMin);
        this.deadheadP90.add(deadheadMin);
        this.executionMedian.add(executionMin);
        this.executionP90.add(executionMin);
        this.dispatchTotal++;
        if (facBucket) {
          facBucket.deadheadMedian.add(deadheadMin);
          facBucket.executionMedian.add(executionMin);
          facBucket.dispatchTotal++;
        }
      }
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

    // QBR Analysis: Overall trend for weekly moves
    const movesOverallTrend = computeOverallTrend(
      { weekly: this.movesByWeek, daily: this.movesByDay },
      'Weekly moves'
    );
    if (movesOverallTrend && movesOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(movesOverallTrend, {
        increaseLevel: 'green',   // More moves is generally good (activity)
        decreaseLevel: 'yellow',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Period-over-period comparison for moves
    const movesPeriodComparison = computePeriodComparison(
      { weekly: this.movesByWeek, daily: this.movesByDay },
      'Weekly moves',
      { significantChangePct: 15 }
    );
    if (movesPeriodComparison?.isSignificant) {
      const finding = formatPeriodComparisonFinding(movesPeriodComparison, {
        increaseLevel: 'green',
        decreaseLevel: 'yellow',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Peak/low periods for moves
    const movesPeakLow = findPeakAndLowPeriods(
      { weekly: this.movesByWeek, daily: this.movesByDay },
      'Weekly moves',
      { higherIsBetter: true } // More moves is better
    );
    if (movesPeakLow) {
      const finding = formatPeakLowFinding(movesPeakLow, {
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // Operational context: Recent state for moves
    const movesRecentState = computeTrendAnalysis(
      { weekly: this.movesByWeek, daily: this.movesByDay },
      'Weekly moves (recent)',
      { significantChangePct: 15 }
    );
    if (movesRecentState?.isSignificant) {
      const finding = formatTrendFinding(movesRecentState, {
        increaseLevel: 'green',
        decreaseLevel: 'yellow',
        dataQualityFactors: dqFactors
      });
      if (finding) {
        finding.text = `Recent state: ${finding.text}`;
        findings.push(finding);
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

    // Dispatch efficiency finding (deadhead ratio)
    const deadheadMed = this.deadheadMedian.value();
    const executionMed = this.executionMedian.value();
    let deadheadRatio = null;
    if (Number.isFinite(deadheadMed) && Number.isFinite(executionMed) && this.dispatchTotal >= 20) {
      const totalMed = deadheadMed + executionMed;
      deadheadRatio = totalMed > 0 ? Math.round((deadheadMed / totalMed) * 100) : null;

      if (deadheadRatio !== null) {
        if (deadheadRatio > 50) {
          findings.push({
            level: 'yellow',
            text: `Drivers spending ${deadheadRatio}% of move time traveling to trailers.`,
            confidence: 'medium',
            confidenceReason: generateConfidenceReason('medium', { ...dqFactors, sampleSize: this.dispatchTotal })
          });
          recs.push('Review dispatch logic - Loss of productivity when drivers are assigned trailers far from their current location.');
        } else {
          findings.push({
            level: 'green',
            text: `Dispatch efficiency ${deadheadRatio <= 30 ? 'looks healthy' : 'is reasonable'} (${deadheadRatio}% deadhead).`,
            confidence: 'medium',
            confidenceReason: generateConfidenceReason('medium', { ...dqFactors, sampleSize: this.dispatchTotal })
          });
        }
      }
    }

    // Compliance recommendation (linked to data quality issue)
    if (compliancePct !== null && compliancePct < 30) {
      recs.push('Recommend retraining on driver workflow (accept/start/complete), and validate device connectivity + timestamp capture.');
    }

    // Aggregate facility-level issues for "All Facilities" view
    if (this.byFacility.size > 1) {
      const facilityIssues = {
        lowCompliance: [],
        highDeadhead: [],
        highQueueTime: []
      };

      for (const [facilityName, bucket] of this.byFacility) {
        // Calculate facility metrics
        const facCompliancePct = bucket.complianceTotal > 0
          ? Math.round((bucket.complianceOk / bucket.complianceTotal) * 100)
          : null;
        const facDeadheadPct = this.calculateFacilityDeadheadPct(bucket);
        const facQueueMed = bucket.queueTotal > 0 ? bucket.queueMedian?.value() : null;

        if (facCompliancePct !== null && facCompliancePct < 70) {
          facilityIssues.lowCompliance.push({ name: facilityName, value: facCompliancePct });
        }
        if (facDeadheadPct !== null && facDeadheadPct > 40) {
          facilityIssues.highDeadhead.push({ name: facilityName, value: facDeadheadPct });
        }
        if (facQueueMed !== null && facQueueMed > 15) {
          facilityIssues.highQueueTime.push({ name: facilityName, value: Math.round(facQueueMed) });
        }
      }

      // Add aggregated findings
      if (facilityIssues.lowCompliance.length > 0) {
        const worst = facilityIssues.lowCompliance.sort((a, b) => a.value - b.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: worst[0].value < 50 ? 'red' : 'yellow',
          text: `${facilityIssues.lowCompliance.length} of ${this.byFacility.size} facilities have low compliance rates (<70%): ${names}. Lowest: ${worst[0].name} at ${worst[0].value}%.`,
          confidence: 'high',
          confidenceReason: 'Based on compliance rate per facility.'
        });
        recs.push(`Prioritize driver training at low-compliance facilities: ${names}.`);
      }

      if (facilityIssues.highDeadhead.length > 0) {
        const worst = facilityIssues.highDeadhead.sort((a, b) => b.value - a.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: 'yellow',
          text: `${facilityIssues.highDeadhead.length} of ${this.byFacility.size} facilities have high deadhead percentages (>40%): ${names}. Highest: ${worst[0].name} at ${worst[0].value}%.`,
          confidence: 'medium',
          confidenceReason: 'Based on deadhead percentage per facility.'
        });
        recs.push(`Review dispatch optimization at high-deadhead facilities: ${names}.`);
      }

      if (facilityIssues.highQueueTime.length > 0) {
        const worst = facilityIssues.highQueueTime.sort((a, b) => b.value - a.value);
        const names = worst.map(f => f.name).join(', ');
        findings.push({
          level: 'yellow',
          text: `${facilityIssues.highQueueTime.length} of ${this.byFacility.size} facilities have high queue times (>15 min): ${names}. Highest: ${worst[0].name} at ${worst[0].value} min.`,
          confidence: 'medium',
          confidenceReason: 'Based on median queue time per facility.'
        });
        recs.push(`Investigate queue bottlenecks at: ${names}.`);
      }
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
        deadhead_median_minutes: Number.isFinite(deadheadMed) ? Math.round(deadheadMed * 10) / 10 : null,
        execution_median_minutes: Number.isFinite(executionMed) ? Math.round(executionMed * 10) / 10 : null,
        deadhead_ratio_pct: deadheadRatio,
        // Derived "moves per driver per day" (approx)
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

  /**
   * Finalize results for a specific facility.
   * Returns a simplified result with key metrics for tabbed display.
   */
  finalizeFacility(facility, meta) {
    // Normalize facility name to ensure consistent lookup with how buckets are stored
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;

    const bucket = this.byFacility.get(normalized);
    if (!bucket) return null;

    // Calculate key metrics from bucket
    const avgMovesPerDriverPerDay = deriveMovesPerDriverPerDay(bucket.movesByDay, bucket.activeDriversByDay);
    const compliancePct = bucket.complianceTotal > 0
      ? Math.round((bucket.complianceOk / bucket.complianceTotal) * 100)
      : null;
    const medQueue = bucket.queueTotal > 0 ? bucket.queueMedian.value() : null;
    const queueP90 = bucket.queueTotal > 0 ? bucket.queueP90.value() : null;
    const deadheadMed = bucket.dispatchTotal > 0 ? bucket.deadheadMedian.value() : null;
    const executionMed = bucket.dispatchTotal > 0 ? bucket.executionMedian.value() : null;
    const deadheadRatio = this.calculateFacilityDeadheadPct(bucket);

    // Simplified data quality
    const dq = bucket.totalRows > 0 ? 75 : 0;

    // Build simple charts
    const charts = [];

    // Active drivers & moves over time (matching "All Facilities" format)
    if (bucket.movesByDay && bucket.movesByDay.map.size > 0) {
      const labels = Array.from(bucket.movesByDay.map.keys()).sort();
      const movesData = labels.map(l => bucket.movesByDay.map.get(l) || 0);
      const activeData = labels.map(l => bucket.activeDriversByDay?.get(l)?.estimate() || 0);

      charts.push({
        id: 'active_drivers_and_moves_daily',
        title: 'Active drivers & moves (daily)',
        kind: 'line',
        description: 'Daily trend using approximate distinct counting (no raw driver lists stored).',
        data: {
          labels,
          datasets: [
            { label: 'Active drivers (approx)', data: activeData },
            { label: 'Moves', data: movesData }
          ]
        },
        csv: {
          columns: ['day', 'active_drivers_approx', 'moves'],
          rows: labels.map((t, i) => ({
            day: t,
            active_drivers_approx: activeData[i],
            moves: movesData[i]
          }))
        }
      });
    }

    // Top drivers chart
    if (bucket.movesByDriver && bucket.movesByDriver.map.size > 0) {
      const top10 = bucket.movesByDriver.top(10);
      charts.push({
        id: 'top_drivers',
        title: 'Top Drivers by Moves',
        kind: 'bar',
        data: {
          labels: top10.map(x => x.key),
          datasets: [{
            label: 'Moves',
            data: top10.map(x => x.value)
          }]
        }
      });
    }

    // Generate findings and recommendations
    const findings = [];
    const recs = [];

    // Compliance findings
    if (compliancePct !== null) {
      if (compliancePct >= 90) {
        findings.push({
          level: 'green',
          text: `${facility} has excellent compliance rate of ${compliancePct}%.`,
          confidence: 'high',
        });
      } else if (compliancePct >= 70) {
        findings.push({
          level: 'yellow',
          text: `${facility} has moderate compliance rate of ${compliancePct}%.`,
          confidence: 'high',
        });
        recs.push(`Review driver training at ${facility} to improve compliance.`);
      } else {
        findings.push({
          level: 'red',
          text: `${facility} has low compliance rate of ${compliancePct}%.`,
          confidence: 'high',
        });
        recs.push(`Investigate compliance issues at ${facility} and consider additional training.`);
      }
    }

    // Deadhead findings
    if (deadheadRatio !== null && deadheadRatio > 40) {
      findings.push({
        level: 'yellow',
        text: `${facility} has high deadhead percentage of ${deadheadRatio}% indicating inefficient dispatching.`,
        confidence: 'medium',
      });
      recs.push(`Optimize dispatching at ${facility} to reduce deadhead time.`);
    }

    // Queue time findings
    if (medQueue !== null && medQueue > 15) {
      findings.push({
        level: 'yellow',
        text: `${facility} has median queue time of ${Math.round(medQueue)} minutes.`,
        confidence: 'medium',
      });
      recs.push(`Review queue management at ${facility} to reduce driver wait times.`);
    }

    // Get top drivers for ROI calculation
    const topDrivers = bucket.movesByDriver ? bucket.movesByDriver.top(10) : [];

    return {
      report: 'driver_history',
      facility,
      meta,
      dataQuality: {
        score: dq,
        label: dq >= 80 ? 'High' : dq >= 50 ? 'Medium' : 'Low',
        color: dq >= 80 ? 'green' : dq >= 50 ? 'yellow' : 'red',
        totalRows: bucket.totalRows,
      },
      metrics: {
        moves_total: bucket.movesTotal,
        compliance_pct: compliancePct,
        queue_median_minutes: Number.isFinite(medQueue) ? Math.round(medQueue * 10) / 10 : null,
        queue_p90_minutes: Number.isFinite(queueP90) ? Math.round(queueP90 * 10) / 10 : null,
        deadhead_median_minutes: Number.isFinite(deadheadMed) ? Math.round(deadheadMed * 10) / 10 : null,
        execution_median_minutes: Number.isFinite(executionMed) ? Math.round(executionMed * 10) / 10 : null,
        deadhead_ratio_pct: deadheadRatio,
        avg_moves_per_driver_per_day: avgMovesPerDriverPerDay,
      },
      charts,
      findings,
      recommendations: recs,
      roi: computeLaborROIIfEnabled({
        meta,
        metrics: {
          movesTotal: bucket.movesTotal,
          avgMovesPerDriverPerDay,
          topDrivers,
          movesByDriver: bucket.movesByDriver,
          movesByDay: bucket.movesByDay,
          activeDriversByDay: bucket.activeDriversByDay,
          totalDays: bucket.movesByDay?.map?.size || 0,
        },
        assumptions: meta.assumptions
      }),
    };
  }

  // Calculate deadhead percentage for a facility bucket
  calculateFacilityDeadheadPct(bucket) {
    if (!bucket.dispatchTotal || bucket.dispatchTotal === 0) return null;
    const medDeadhead = bucket.deadheadMedian?.value() ?? 0;
    const medExecution = bucket.executionMedian?.value() ?? 0;
    const total = medDeadhead + medExecution;
    if (total === 0) return null;
    return Math.round((medDeadhead / total) * 100);
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

    // Drilldown data for lost events by carrier (only collected when enableDrilldown is true)
    this.lostByCarrierDrilldown = new Map(); // carrier -> array of { trailer, eventDate, eventType }
  }

  /**
   * Create a per-facility metrics bucket for trailer history.
   */
  createFacilityBucket() {
    return {
      totalRows: 0,
      lostCount: 0,
      errorCounts: {
        trailer_marked_lost: 0,
        yard_check_insert: 0,
        spot_edited: 0,
        facility_edited: 0,
      },
      errorsByPeriod: new CounterMap(),
      daysWithData: new Set(),
      lostByCarrier: new CounterMap(),
    };
  }

  ingest({ row, flags }) {
    this.totalRows++;

    // Track facility for multi-facility support
    const facility = row._facility || flags?.facility || '';
    const facBucket = this.trackFacility(facility, 'trailer_history');
    if (facBucket) {
      facBucket.totalRows++;
    }

    const event = safeStr(firstPresent(row, ['event', 'event_type', 'event_name', 'event_string', 'action', 'status_change']));
    if (event) this.eventTypes.inc(event);

    const dt = parseTimestamp(firstPresent(row, ['event_time', 'created_at', 'timestamp', 'event_timestamp']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    if (dt) {
      this.parseOk++;
      this.trackDate(dt); // Track for date range inference
      this.daysWithData.add(dayKey(dt, this.timezone));
      if (facBucket) facBucket.daysWithData.add(dayKey(dt, this.timezone));
    }

    const carrier = row.scac ?? row.carrier_scac ?? row.scac_code ?? row.carrier;

    const isLost = /marked\s+lost|trailer\s+marked\s+lost|\blost\b/i.test(event);
    if (isLost) {
      this.lostCount++;
      this.errorCounts.trailer_marked_lost++;
      if (facBucket) {
        facBucket.lostCount++;
        facBucket.errorCounts.trailer_marked_lost++;
      }
      const carrierKey = isNil(carrier) ? 'Unknown' : carrier;
      if (!isNil(carrier)) this.lostByCarrier.inc(carrier);
      if (!isNil(carrier) && facBucket) facBucket.lostByCarrier.inc(carrier);
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.byWeek.inc(weekKey(dt, this.timezone));
        this.byMonth.inc(monthKey(dt, this.timezone));
        this.byDay.inc(dk);
        this.errorsByPeriod.inc(dk);
        this.lostByDay.inc(dk);
        if (facBucket) facBucket.errorsByPeriod.inc(dk);
      }

      // Collect drilldown data if enabled
      if (this.enableDrilldown) {
        const trailer = safeStr(row.trailer_number || row.trailer_id || row.equipment_number || row.trailer || '');
        const eventDate = dt ? dt.toFormat('yyyy-MM-dd HH:mm') : '';
        if (!this.lostByCarrierDrilldown.has(carrierKey)) {
          this.lostByCarrierDrilldown.set(carrierKey, []);
        }
        this.lostByCarrierDrilldown.get(carrierKey).push({ trailer, eventDate, eventType: event });
      }
    }

    // Track other error-indicating events
    const isYardCheckInsert = /yard\s*check\s*insert/i.test(event);
    if (isYardCheckInsert) {
      this.errorCounts.yard_check_insert++;
      if (facBucket) facBucket.errorCounts.yard_check_insert++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.yardCheckInsertByDay.inc(dk);
        if (facBucket) facBucket.errorsByPeriod.inc(dk);
      }
    }

    const isSpotEdited = /spot\s*edited/i.test(event);
    if (isSpotEdited) {
      this.errorCounts.spot_edited++;
      if (facBucket) facBucket.errorCounts.spot_edited++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.spotEditedByDay.inc(dk);
        if (facBucket) facBucket.errorsByPeriod.inc(dk);
      }
    }

    const isFacilityEdited = /facility\s*edited/i.test(event);
    if (isFacilityEdited) {
      this.errorCounts.facility_edited++;
      if (facBucket) facBucket.errorCounts.facility_edited++;
      if (dt) {
        const dk = dayKey(dt, this.timezone);
        this.errorsByPeriod.inc(dk);
        this.facilityEditedByDay.inc(dk);
        if (facBucket) facBucket.errorsByPeriod.inc(dk);
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
      dataQualityFindings.push({ level: 'info', text: 'No "Trailer marked lost" events found.' });
      recs.push('If lost events are expected but missing, confirm event strings and report configuration match local processes.');
    }

    // QBR Analysis: Overall trend for lost events
    const lostOverallTrend = computeOverallTrend(
      { monthly: this.byMonth, weekly: this.byWeek, daily: this.byDay },
      'Lost trailer events'
    );
    if (lostOverallTrend && lostOverallTrend.stability !== 'volatile') {
      const finding = formatOverallTrendFinding(lostOverallTrend, {
        increaseLevel: 'red',     // More lost is bad
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);
    }

    // QBR Analysis: Period-over-period comparison for lost events
    const lostPeriodComparison = computePeriodComparison(
      { monthly: this.byMonth, weekly: this.byWeek, daily: this.byDay },
      'Lost trailer events',
      { significantChangePct: 25 }
    );
    if (lostPeriodComparison?.isSignificant) {
      const finding = formatPeriodComparisonFinding(lostPeriodComparison, {
        increaseLevel: 'red',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);

      if (lostPeriodComparison.direction === 'increased') {
        recs.push('Lost trailer events increased over the period - investigate carrier handoffs and check-out habits.');
      }
    }

    // QBR Analysis: Peak/low periods for lost events
    const lostPeakLow = findPeakAndLowPeriods(
      { monthly: this.byMonth, weekly: this.byWeek, daily: this.byDay },
      'Lost trailer events',
      { higherIsBetter: false } // Lower lost events is better
    );
    if (lostPeakLow) {
      const finding = formatPeakLowFinding(lostPeakLow, {
        dataQualityFactors: dqFactors
      });
      if (finding) findings.push(finding);

      // Add recommendation if weekend pattern detected
      if (lostPeakLow.weekendPattern) {
        if (lostPeakLow.weekendPattern.type === 'worst') {
          recs.push('Weekend lost events are consistently higher - review weekend check-in/check-out processes.');
        }
      }
    }

    // Operational context: Recent state for lost events
    const lostRecentState = computeTrendAnalysis(
      { monthly: this.byMonth, weekly: this.byWeek, daily: this.byDay },
      'Lost trailer events (recent)',
      { significantChangePct: 25 }
    );
    if (lostRecentState?.isSignificant) {
      const finding = formatTrendFinding(lostRecentState, {
        increaseLevel: 'red',
        decreaseLevel: 'green',
        dataQualityFactors: dqFactors
      });
      if (finding) {
        finding.text = `Recent state: ${finding.text}`;
        findings.push(finding);
      }
    }

    // Volume finding when no trend available but events exist
    if (!lostRecentState && this.lostCount > 0) {
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
          text: `Excellent: Only ${this.lostCount} "Trailer marked lost" ${this.lostCount === 1 ? 'event' : 'events'} detected - low count indicates good location tracking.`,
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
        // Error rate as percentage of total events (for health score)
        error_rate_pct: this.totalRows > 0
          ? Math.round((totalErrors / this.totalRows) * 1000) / 10  // Round to 1 decimal
          : null,
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
          },
          drilldown: this.enableDrilldown ? {
            columns: ['trailer', 'eventDate', 'eventType'],
            columnLabels: ['Trailer #', 'Event Date', 'Event Type'],
            byLabel: Object.fromEntries(
              topCarriers.map(x => [x.key, this.lostByCarrierDrilldown.get(x.key) || []])
            )
          } : null
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

  /**
   * Finalize results for a specific facility.
   * Returns a simplified result with key metrics for tabbed display.
   */
  finalizeFacility(facility, meta) {
    // Normalize facility name to ensure consistent lookup with how buckets are stored
    const normalized = FacilityRegistry.normalizeFacilityName(facility);
    if (!normalized) return null;

    const bucket = this.byFacility.get(normalized);
    if (!bucket) return null;

    // Calculate total errors
    const totalErrors = (bucket.errorCounts?.trailer_marked_lost || 0) +
      (bucket.errorCounts?.yard_check_insert || 0) +
      (bucket.errorCounts?.spot_edited || 0) +
      (bucket.errorCounts?.facility_edited || 0);

    // Calculate error rate (errors per day)
    const daysWithData = bucket.daysWithData?.size || 0;
    const errorRate = daysWithData > 0 ? Math.round((totalErrors / daysWithData) * 10) / 10 : null;

    // Simplified data quality
    const dq = bucket.totalRows > 0 ? 75 : 0;

    // Build simple charts
    const charts = [];

    // Error events over time if available
    if (bucket.errorsByPeriod && bucket.errorsByPeriod.map.size > 0) {
      const labels = Array.from(bucket.errorsByPeriod.map.keys()).sort();
      const errorData = labels.map(l => bucket.errorsByPeriod.map.get(l) || 0);

      charts.push({
        id: 'errors_daily',
        title: 'Error Events per Day',
        kind: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'Errors',
            data: errorData
          }]
        }
      });
    }

    // Top carriers by lost events (matching "All Facilities" format)
    if (bucket.lostByCarrier && bucket.lostByCarrier.map.size > 0) {
      const topCarriers = bucket.lostByCarrier.top(8);
      charts.push({
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
      });
    }

    // Generate findings and recommendations
    const findings = [];
    const recs = [];

    // Lost trailer findings
    const lostCount = bucket.errorCounts?.trailer_marked_lost || 0;
    if (lostCount > 5) {
      findings.push({
        level: 'red',
        text: `${facility} has ${lostCount} trailers marked as lost.`,
        confidence: 'high',
      });
      recs.push(`Investigate lost trailer events at ${facility} and review tracking procedures.`);
    } else if (lostCount > 0) {
      findings.push({
        level: 'yellow',
        text: `${facility} has ${lostCount} trailer(s) marked as lost.`,
        confidence: 'high',
      });
    }

    // Error rate findings
    if (errorRate !== null && errorRate > 5) {
      findings.push({
        level: 'yellow',
        text: `${facility} has elevated error rate of ${errorRate} errors per day.`,
        confidence: 'medium',
      });
      recs.push(`Review data entry practices at ${facility} to reduce error events.`);
    } else if (errorRate !== null && errorRate <= 1) {
      findings.push({
        level: 'green',
        text: `${facility} has low error rate of ${errorRate} errors per day.`,
        confidence: 'medium',
      });
    }

    // Get errors by period array for ROI
    const errorsByPeriodArray = bucket.errorsByPeriod ?
      Array.from(bucket.errorsByPeriod.map.entries())
        .map(([period, count]) => ({ period, count }))
        .sort((a, b) => a.period.localeCompare(b.period)) : [];

    return {
      report: 'trailer_history',
      facility,
      meta,
      dataQuality: {
        score: dq,
        label: dq >= 80 ? 'High' : dq >= 50 ? 'Medium' : 'Low',
        color: dq >= 80 ? 'green' : dq >= 50 ? 'yellow' : 'red',
        totalRows: bucket.totalRows,
      },
      metrics: {
        total_error_events: totalErrors,
        trailer_marked_lost: bucket.errorCounts?.trailer_marked_lost || 0,
        yard_check_insert: bucket.errorCounts?.yard_check_insert || 0,
        spot_edited: bucket.errorCounts?.spot_edited || 0,
        facility_edited: bucket.errorCounts?.facility_edited || 0,
        errors_per_day: errorRate,
        // Error rate as percentage of total events (for health score)
        error_rate_pct: bucket.totalRows > 0
          ? Math.round((totalErrors / bucket.totalRows) * 1000) / 10  // Round to 1 decimal
          : null,
      },
      charts,
      findings,
      recommendations: recs,
      roi: computeTrailerErrorRateAnalysis({
        metrics: {
          errorCounts: bucket.errorCounts || {},
          errorsByPeriod: errorsByPeriodArray,
          totalRows: bucket.totalRows,
          totalDays: daysWithData,
          granularity: 'day',
        },
      }),
    };
  }
}

// ---------- Factory ----------
export function createAnalyzers({ timezone, startDate, endDate, assumptions, onWarning, isCSVMode = false, enableDrilldown = true }) {
  const base = { timezone, startDate, endDate, assumptions, onWarning, enableDrilldown };
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
  const costPerHour = a.detention_cost_per_hour;

  // Only need detention cost per hour for this calculation
  if (!Number.isFinite(costPerHour)) return null;

  const prevented = metrics.prevented || 0;
  const detentionHoursArray = metrics.detentionHoursArray || [];
  const detentionEventsWithDeparture = metrics.detentionEventsWithDeparture || 0;
  const totalDetentionHours = metrics.totalDetentionHours || 0;

  // Calculate median detention hours from actual detention events
  let medianDetentionHours = 0;
  let avgDetentionHours = 0;
  let calculationNote = '';

  if (detentionHoursArray.length > 0) {
    // Use IQR (Interquartile Range) method to detect and filter outliers
    // This statistically adapts to the actual data distribution rather than using a hardcoded threshold
    // Note: Actual detention spend still uses real hours, this only affects prevention estimates
    const sorted = [...detentionHoursArray].sort((a, b) => a - b);

    // Calculate quartiles
    const q1Index = Math.floor(sorted.length * 0.25);
    const q3Index = Math.floor(sorted.length * 0.75);
    const q1 = sorted[q1Index];
    const q3 = sorted[q3Index];
    const iqr = q3 - q1;

    // Standard outlier definition: values beyond Q3 + 1.5*IQR
    // This is the same method used in box plots
    const upperBound = q3 + (1.5 * iqr);

    // Filter outliers
    const filtered = detentionHoursArray.filter(h => h <= upperBound);
    const outlierCount = detentionHoursArray.length - filtered.length;

    // Calculate median from filtered data (or full data if filtering removed too much)
    // Require at least 50% of original data to use filtered set
    const dataForMedian = filtered.length >= detentionHoursArray.length * 0.5
      ? filtered
      : detentionHoursArray;

    const sortedForMedian = [...dataForMedian].sort((a, b) => a - b);
    const mid = Math.floor(sortedForMedian.length / 2);
    medianDetentionHours = sortedForMedian.length % 2 === 0
      ? (sortedForMedian[mid - 1] + sortedForMedian[mid]) / 2
      : sortedForMedian[mid];

    // Calculate average from all events
    avgDetentionHours = totalDetentionHours / detentionEventsWithDeparture;

    // Build calculation note
    if (outlierCount > 0 && filtered.length >= detentionHoursArray.length * 0.5) {
      calculationNote = `Based on ${filtered.length} detention events (${outlierCount} outliers >${round1(upperBound)}hrs excluded using IQR method). Median: ${round1(medianDetentionHours)} hrs, Avg: ${round1(avgDetentionHours)} hrs`;
    } else {
      calculationNote = `Based on ${detentionEventsWithDeparture} actual detention events (median: ${round1(medianDetentionHours)} hrs, avg: ${round1(avgDetentionHours)} hrs)`;
    }
  } else {
    // Fallback: use 2 hours as reasonable estimate if no actual detention data
    medianDetentionHours = 2.0;
    calculationNote = 'No completed detention events yet. Using estimated 2 hours per event as baseline.';
  }

  // Calculate hours avoided by using median
  const hoursAvoided = prevented * medianDetentionHours;
  const costAvoided = hoursAvoided * costPerHour;

  // Get time period breakdown
  const monthlyPrevented = metrics.monthlyPrevented;
  const weeklyPrevented = metrics.weeklyPrevented;
  const dailyPrevented = metrics.dailyPrevented;

  // Determine which granularity to show
  let periodBreakdown = [];
  if (monthlyPrevented && monthlyPrevented.map && monthlyPrevented.map.size > 0) {
    const sorted = Array.from(monthlyPrevented.map.entries())
      .sort(([a], [b]) => a.localeCompare(b));
    periodBreakdown = sorted.map(([period, count]) => ({
      period,
      count,
      hoursAvoided: round1(count * medianDetentionHours),
      costAvoided: Math.round(count * medianDetentionHours * costPerHour)
    }));
  } else if (weeklyPrevented && weeklyPrevented.map && weeklyPrevented.map.size > 0) {
    const sorted = Array.from(weeklyPrevented.map.entries())
      .sort(([a], [b]) => a.localeCompare(b));
    periodBreakdown = sorted.map(([period, count]) => ({
      period,
      count,
      hoursAvoided: round1(count * medianDetentionHours),
      costAvoided: Math.round(count * medianDetentionHours * costPerHour)
    }));
  } else if (dailyPrevented && dailyPrevented.map && dailyPrevented.map.size > 0) {
    const sorted = Array.from(dailyPrevented.map.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .slice(-30); // Show last 30 days max
    periodBreakdown = sorted.map(([period, count]) => ({
      period,
      count,
      hoursAvoided: round1(count * medianDetentionHours),
      costAvoided: Math.round(count * medianDetentionHours * costPerHour)
    }));
  }

  // Build insights array (not wall of text)
  const insights = [];

  if (prevented > 0) {
    insights.push(`Prevention success: ${prevented} detention events prevented this period`);
    insights.push(`Median detention duration: ${round1(medianDetentionHours)} hours (from actual detention events)`);
    insights.push(`Estimated detention hours avoided: ${round1(hoursAvoided)} hours`);
    insights.push(`Detention cost avoided: $${Math.round(costAvoided).toLocaleString()} (${prevented} events × ${round1(medianDetentionHours)} hrs × $${costPerHour}/hr)`);
    if (calculationNote) {
      insights.push(calculationNote);
    }
  } else {
    insights.push('No prevented detention events recorded this period.');
    insights.push('Prevented detention occurs when trailers depart after pre-detention alert but before detention threshold.');
  }

  return {
    label: 'Prevented Detention Value',
    assumptionsUsed: {
      detention_cost_per_hour: costPerHour,
      median_detention_hours: round1(medianDetentionHours),
    },
    estimate: {
      prevented_detention_events: prevented,
      median_detention_hours: round1(medianDetentionHours),
      avg_detention_hours: round1(avgDetentionHours),
      estimated_hours_avoided: round1(hoursAvoided),
      estimated_cost_avoided: Math.round(costAvoided * 100) / 100,
    },
    insights,
    periodBreakdown, // Array of { period, count, hoursAvoided, costAvoided }
    disclaimer: detentionHoursArray.length > 0
      ? 'Calculation uses median detention duration from actual detention events to estimate value of prevention.'
      : 'No completed detention events yet. Using estimated baseline for prevention value calculation.',
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

  // 6. Driver headcount trend analysis (first half vs second half of period)
  if (activeDriversByDay && activeDriversByDay.size >= 14) {
    // Need at least 14 days for meaningful comparison (7 days each half)
    const sortedDays = Array.from(activeDriversByDay.keys()).sort();
    const midpoint = Math.floor(sortedDays.length / 2);

    const firstHalfDays = sortedDays.slice(0, midpoint);
    const secondHalfDays = sortedDays.slice(midpoint);

    // Calculate average driver count for each half
    let firstHalfTotal = 0;
    for (const day of firstHalfDays) {
      firstHalfTotal += activeDriversByDay.get(day)?.estimate() || 0;
    }
    const firstHalfAvg = round1(firstHalfTotal / firstHalfDays.length);

    let secondHalfTotal = 0;
    for (const day of secondHalfDays) {
      secondHalfTotal += activeDriversByDay.get(day)?.estimate() || 0;
    }
    const secondHalfAvg = round1(secondHalfTotal / secondHalfDays.length);

    // Store trend data
    staffingAnalysis.driverTrend = {
      firstHalfAvg,
      secondHalfAvg,
      firstHalfPeriod: `${firstHalfDays[0]} to ${firstHalfDays[firstHalfDays.length - 1]}`,
      secondHalfPeriod: `${secondHalfDays[0]} to ${secondHalfDays[secondHalfDays.length - 1]}`,
    };

    // Only highlight decreases (per user request - don't mention increases)
    if (firstHalfAvg > secondHalfAvg && secondHalfAvg > 0) {
      const reduction = round1(firstHalfAvg - secondHalfAvg);
      const reductionPct = Math.round(((firstHalfAvg - secondHalfAvg) / firstHalfAvg) * 100);

      // Only report if meaningful reduction (>10%)
      if (reductionPct >= 10) {
        insights.push(`Driver headcount decreased from ~${firstHalfAvg} to ~${secondHalfAvg} drivers/day (${reductionPct}% reduction) between first and second half of the period`);

        // Calculate labor savings if labor rate is provided (not using default)
        const hasLaborRate = Number.isFinite(a.labor_fully_loaded_rate_per_hour);
        if (hasLaborRate) {
          // Annual savings = reduction in drivers × hours/day × rate × working days/year
          const workingDaysPerYear = 260; // ~5 days/week × 52 weeks
          const annualSavings = Math.round(reduction * driverDayHours * laborRate * workingDaysPerYear);
          const formattedSavings = annualSavings.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
          insights.push(`Estimated annual labor savings from ${round1(reduction)} fewer drivers: ${formattedSavings} (${reduction} drivers × ${driverDayHours} hrs × $${laborRate}/hr × ${workingDaysPerYear} days/yr)`);
        }

        // Check if productivity was maintained despite fewer drivers
        if (movesByDay && movesByDay.map.size >= 14) {
          let firstHalfMoves = 0;
          for (const day of firstHalfDays) {
            firstHalfMoves += movesByDay.map.get(day) || 0;
          }
          const firstHalfMovesPerDay = round1(firstHalfMoves / firstHalfDays.length);

          let secondHalfMoves = 0;
          for (const day of secondHalfDays) {
            secondHalfMoves += movesByDay.map.get(day) || 0;
          }
          const secondHalfMovesPerDay = round1(secondHalfMoves / secondHalfDays.length);

          staffingAnalysis.driverTrend.firstHalfMovesPerDay = firstHalfMovesPerDay;
          staffingAnalysis.driverTrend.secondHalfMovesPerDay = secondHalfMovesPerDay;

          // Calculate moves per driver for each period
          const firstHalfMovesPerDriver = firstHalfAvg > 0 ? round1(firstHalfMovesPerDay / firstHalfAvg) : 0;
          const secondHalfMovesPerDriver = secondHalfAvg > 0 ? round1(secondHalfMovesPerDay / secondHalfAvg) : 0;

          if (secondHalfMovesPerDriver >= firstHalfMovesPerDriver * 0.95) {
            // Productivity maintained or improved
            if (secondHalfMovesPerDriver > firstHalfMovesPerDriver * 1.05) {
              const improvementPct = Math.round(((secondHalfMovesPerDriver / firstHalfMovesPerDriver) - 1) * 100);
              insights.push(`Productivity improved ${improvementPct}% (${firstHalfMovesPerDriver} → ${secondHalfMovesPerDriver} moves/driver/day) despite fewer drivers`);
            } else {
              insights.push(`Productivity maintained at ~${secondHalfMovesPerDriver} moves/driver/day despite fewer drivers`);
            }
          }
        }
      }
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
        insights.push(`Opportunity cost: ~$${round1(dailyGapValue)}/day in unrealized capacity (${round1(gap)} turns/day gap × $${round1(costPerTurn)}/turn [${costPerHour}/hr × ${hoursPerDay}hrs ÷ ${target} target] × ${uniqueDoors || 1} doors)`);
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

  // Apply IQR method to identify outliers
  const detentionHoursArray = metrics.detentionHoursArray || [];
  let outlierInfo = null;

  if (detentionHoursArray.length > 0) {
    const sorted = [...detentionHoursArray].sort((a, b) => a - b);

    // Calculate quartiles
    const q1Index = Math.floor(sorted.length * 0.25);
    const q3Index = Math.floor(sorted.length * 0.75);
    const q1 = sorted[q1Index];
    const q3 = sorted[q3Index];
    const iqr = q3 - q1;
    const upperBound = q3 + (1.5 * iqr);

    // Identify outliers
    const outliers = detentionHoursArray.filter(h => h > upperBound);

    if (outliers.length > 0) {
      const filteredHours = detentionHoursArray.filter(h => h <= upperBound);
      const totalFilteredHours = filteredHours.reduce((sum, h) => sum + h, 0);
      const spendWithoutOutliers = totalFilteredHours * costPerHour;
      const outlierHours = outliers.reduce((sum, h) => sum + h, 0);

      outlierInfo = {
        count: outliers.length,
        upperBound: round1(upperBound),
        totalOutlierHours: round1(outlierHours),
        spendWithoutOutliers: Math.round(spendWithoutOutliers),
        filteredCount: filteredHours.length
      };
    }
  }

  // Build insights with actual and adjusted spend
  insights.push(`Detention spend this period: $${Math.round(detentionSpend).toLocaleString()} (${detentionEvents} trailers, ${round1(totalDetentionHours)} total hours)`);

  if (outlierInfo) {
    insights.push(`    - Actual spend (including outliers): $${Math.round(detentionSpend).toLocaleString()}`);
    insights.push(`    - Adjusted spend (excluding ${outlierInfo.count} outliers >${outlierInfo.upperBound}hrs): $${outlierInfo.spendWithoutOutliers.toLocaleString()}`);
    insights.push(`    - Outlier impact: ${outlierInfo.count} trailers with ${outlierInfo.totalOutlierHours} hours = $${Math.round(detentionSpend - outlierInfo.spendWithoutOutliers).toLocaleString()} additional cost`);
  }

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
        insights.push(`Opportunity cost: ~$${round1(dailyGapValue)}/day in unrealized capacity (${round1(gap)} turns/day gap × $${round1(costPerTurn)}/turn [${costPerHour}/hr × ${hoursPerDay}hrs ÷ ${target} target] × ${uniqueDoors || 1} doors)`);
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
