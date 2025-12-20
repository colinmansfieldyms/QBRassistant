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
    const dtFast = DateTime.fromObject(fastParts, { zone });
    if (dtFast.isValid) return dtFast;
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
  for (const [k, v] of Object.entries(rawRow)) {
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
  }

  finalize(meta) {
    const trailers = this.totalTrailers;
    const pct = (n, d) => d ? Math.round((n / d) * 1000) / 10 : 0;

    const updatedTotal = Object.values(this.updatedBuckets).reduce((a, b) => a + b, 0);
    const pctUpdated = (bucketLabel) => pct(this.updatedBuckets[bucketLabel], updatedTotal);

    const placeholderRate = this.scacTotal ? pct(this.placeholderScac, this.scacTotal) : 0;

    const outboundInboundRatio = this.inbound ? (this.outbound / this.inbound) : null;

    const missingDriverRate = this.liveLoads ? pct(this.liveLoadMissingDriverContact, this.liveLoads) : null;

    // Findings thresholds (MVP heuristics)
    const findings = [];
    const recs = [];

    // Updated recency
    const stale30 = pctUpdated('30d+');
    if (stale30 >= 25) {
      findings.push({ level: 'red', text: `${stale30}% of inventory records are older than 30 days (updated_at).`, confidence: 'high' });
      recs.push('Review update workflows and integrations to ensure inventory stays current (goal: majority updated within 7–30 days).');
    } else if (stale30 >= 10) {
      findings.push({ level: 'yellow', text: `${stale30}% of inventory records are older than 30 days.`, confidence: 'medium' });
      recs.push('Spot-check stale records and confirm whether they represent inactive assets or missed updates.');
    } else {
      findings.push({ level: 'green', text: 'Inventory recency looks healthy (low share older than 30 days).', confidence: 'high' });
    }

    // SCAC placeholders
    if (placeholderRate >= 10) {
      findings.push({ level: 'yellow', text: `Placeholder/unknown SCAC rate is ${placeholderRate}%.`, confidence: 'high' });
      recs.push('Enforce SCAC validation and/or integrate carrier master data to reduce UNKNOWN/XXXX records.');
    } else {
      findings.push({ level: 'green', text: `SCAC data quality looks solid (placeholder rate ${placeholderRate}%).`, confidence: 'high' });
    }

    // Live load driver contact presence
    if (missingDriverRate !== null) {
      if (missingDriverRate >= 30) {
        findings.push({ level: 'yellow', text: `Live loads missing driver contact presence: ${missingDriverRate}%. (No numbers shown)`, confidence: 'medium' });
        recs.push('If texting is expected, confirm driver contact capture/permissions and train gate/dispatch to populate contact fields.');
      } else {
        findings.push({ level: 'green', text: `Live loads missing driver contact presence is low (${missingDriverRate}%).`, confidence: 'medium' });
      }
    }

    const dq = Math.round(
      0.65 * this.dataQualityScore() +
      0.35 * (updatedTotal ? pct(this.parseOk, this.parseOk + this.parseFails) : 100)
    );
    const badge = scoreToBadge(dq);

    // Charts (2+)
    const moveTypeTop = this.moveType.toObjectSorted();
    const updatedSeries = Object.entries(this.updatedBuckets).map(([bucket, count]) => ({ bucket, count }));

    return {
      report: 'current_inventory',
      meta,
      dataQuality: { score: dq, ...badge, parseOk: this.parseOk, parseFails: this.parseFails, totalRows: this.totalRows },
      metrics: {
        total_trailers: trailers,
        updated_last_24h_pct: pctUpdated('0–1d'),
        updated_last_7d_pct: Math.round(((this.updatedBuckets['0–1d'] + this.updatedBuckets['1–7d']) / (updatedTotal || 1)) * 1000) / 10,
        updated_last_30d_pct: Math.round(((this.updatedBuckets['0–1d'] + this.updatedBuckets['1–7d'] + this.updatedBuckets['7–30d']) / (updatedTotal || 1)) * 1000) / 10,
        placeholder_scac_pct: placeholderRate,
        outbound_vs_inbound_ratio: outboundInboundRatio,
        live_load_missing_driver_contact_pct: missingDriverRate,
      },
      charts: [
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
        {
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
        }
      ],
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

    this.live = 0;
    this.drop = 0;

    this.monthlyDetention = new CounterMap();
    this.monthlyPrevented = new CounterMap();
    this.topScac = new CounterMap();
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

    if (pre) { this.preDetention++; this.parseOk++; }
    if (det) { this.detention++; this.parseOk++; }
    if (pre && !det) this.prevented++;

    const live = normalizeBoolish(row.live_load) ?? (row.live_load == 1);
    if (live === true) this.live++;
    else if (live === false) this.drop++;

    const scac = row.scac ?? row.carrier_scac ?? row.scac_code;
    if (!isNil(scac)) this.topScac.inc(scac);

    if (eventDt) {
      const mk = monthKey(eventDt, this.timezone);
      if (det) this.monthlyDetention.inc(mk);
      if (pre && !det) this.monthlyPrevented.inc(mk);
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    // If zero detention ever
    if (this.detention === 0 && this.preDetention === 0) {
      findings.push({ level: 'yellow', text: 'No detention signals found. This could mean operations are healthy OR the module/config isn’t in use.', confidence: 'medium' });
      recs.push('Recommend a PM/Admin audit: confirm detention configuration, triggers, and report usage.');
    } else if (this.detention === 0 && this.preDetention > 0) {
      findings.push({ level: 'green', text: `Detected ${this.prevented} prevented detention events (pre_detention set, detention_start missing).`, confidence: 'medium' });
      recs.push('Validate the “prevented detention” workflow and ensure carriers/sites align on definitions and timestamps.');
    } else {
      findings.push({ level: 'green', text: `Detention detected: ${this.detention} events; prevented: ${this.prevented}.`, confidence: 'high' });
    }

    const liveDrop = (this.live + this.drop) ? Math.round((this.live / (this.live + this.drop)) * 1000) / 10 : null;
    if (liveDrop !== null && liveDrop > 80) {
      findings.push({ level: 'yellow', text: `Detention history is heavily live-load skewed (~${liveDrop}% live).`, confidence: 'medium' });
      recs.push('If drops are common, confirm drop workflow timestamps are being captured; otherwise this might be expected.');
    }

    const dqBase = this.dataQualityScore();
    const coverage = (this.totalRows ? Math.min(100, Math.round(((this.preDetention + this.detention) / this.totalRows) * 100)) : 100);
    const dq = Math.round(0.6 * dqBase + 0.4 * coverage);
    const badge = scoreToBadge(dq);

    // Charts
    const seriesMonths = mergeMonthSeries(this.monthlyDetention, this.monthlyPrevented);

    return {
      report: 'detention_history',
      meta,
      dataQuality: { score: dq, ...badge, parseOk: this.parseOk, parseFails: this.parseFails, totalRows: this.totalRows, coveragePct: coverage },
      metrics: {
        pre_detention_count: this.preDetention,
        detention_count: this.detention,
        prevented_detention_count: this.prevented,
        live_load_count: this.live,
        drop_load_count: this.drop,
      },
      charts: [
        {
          id: 'detention_vs_prevented_monthly',
          title: 'Detention vs prevented detention (monthly)',
          kind: 'line',
          description: `Monthly counts (timezone-adjusted grouping: ${meta.timezone}).`,
          data: {
            labels: seriesMonths.labels,
            datasets: [
              { label: 'Detention', data: seriesMonths.detention },
              { label: 'Prevented detention', data: seriesMonths.prevented },
            ]
          },
          csv: {
            columns: ['month', 'detention_count', 'prevented_detention_count', 'timezone'],
            rows: seriesMonths.labels.map((m, i) => ({
              month: m,
              detention_count: seriesMonths.detention[i],
              prevented_detention_count: seriesMonths.prevented[i],
              timezone: meta.timezone
            })),
          }
        },
        {
          id: 'detention_live_drop_and_top_scac',
          title: 'Detention split + top SCAC',
          kind: 'bar',
          description: 'Live vs drop counts plus top carriers by occurrences.',
          data: {
            labels: ['Live load', 'Drop load', ...this.topScac.top(5).map(x => x.key)],
            datasets: [{
              label: 'Count',
              data: [this.live, this.drop, ...this.topScac.top(5).map(x => x.value)]
            }]
          },
          csv: {
            columns: ['category', 'count'],
            rows: [
              { category: 'Live load', count: this.live },
              { category: 'Drop load', count: this.drop },
              ...this.topScac.top(5).map(x => ({ category: `SCAC:${x.key}`, count: x.value })),
            ]
          }
        }
      ],
      findings,
      recommendations: recs,
      roi: computeDetentionROIIfEnabled({ meta, metrics: { prevented: this.prevented }, assumptions: meta.assumptions }),
    };
  }
}

class DockDoorHistoryAnalyzer extends BaseAnalyzer {
  constructor(opts) {
    super(opts);

    this.dwellCoverage = { ok: 0, total: 0 };
    this.processCoverage = { ok: 0, total: 0 };

    // month -> P2 estimators
    this.dwellByMonth = new Map();   // key -> { median, p90 }
    this.processByMonth = new Map(); // key -> { median, p90 }

    this.processedBy = new CounterMap();
    this.moveRequestedBy = new CounterMap();
    this.rowsWithRequester = 0;
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
      const mins = dwellEnd.diff(dwellStart, 'minutes').minutes;
      if (Number.isFinite(mins) && mins >= 0) {
        const mk = monthKey(dwellStart, this.timezone);
        const est = this.getEstimators(this.dwellByMonth, mk);
        est.median.add(mins);
        est.p90.add(mins);
      }
    }
    this.dwellCoverage.total++;

    if (procStart && procEnd) {
      this.processCoverage.ok++; this.parseOk++;
      const mins = procEnd.diff(procStart, 'minutes').minutes;
      if (Number.isFinite(mins) && mins >= 0) {
        const mk = monthKey(procStart, this.timezone);
        const est = this.getEstimators(this.processByMonth, mk);
        est.median.add(mins);
        est.p90.add(mins);
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
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    const dwellCoveragePct = this.dwellCoverage.total ? Math.round((this.dwellCoverage.ok / this.dwellCoverage.total) * 1000) / 10 : 0;
    const processCoveragePct = this.processCoverage.total ? Math.round((this.processCoverage.ok / this.processCoverage.total) * 1000) / 10 : 0;

    if (dwellCoveragePct < 60) {
      findings.push({ level: 'yellow', text: `Dwell time coverage is low (${dwellCoveragePct}%).`, confidence: 'medium' });
      recs.push('Confirm dwell start/end timestamps are being recorded consistently (workflow + integrations).');
    } else {
      findings.push({ level: 'green', text: `Dwell time coverage looks usable (${dwellCoveragePct}%).`, confidence: 'high' });
    }

    if (processCoveragePct < 60) {
      findings.push({ level: 'yellow', text: `Process time coverage is low (${processCoveragePct}%).`, confidence: 'medium' });
      recs.push('Confirm process start/end timestamps are being recorded consistently (dock door module usage).');
    } else {
      findings.push({ level: 'green', text: `Process time coverage looks usable (${processCoveragePct}%).`, confidence: 'high' });
    }

    // Adoption concern: move_requested_by dominated by admins
    const topReq = this.moveRequestedBy.top(5);
    const totalReq = Array.from(this.moveRequestedBy.map.values()).reduce((a, b) => a + b, 0);
    const adminLike = topReq
      .filter(x => /admin|system|yms|super/i.test(x.key))
      .reduce((a, b) => a + b.value, 0);
    const adminShare = totalReq ? (adminLike / totalReq) : 0;

    if (totalReq >= 25 && adminShare >= 0.7) {
      findings.push({ level: 'yellow', text: `Move requests appear dominated by admin/system users (~${Math.round(adminShare*100)}%).`, confidence: 'medium' });
      recs.push('If end-user adoption is expected, review requester workflows, roles, and training (goal: requests driven by ops users).');
    }

    const dqBase = this.dataQualityScore();
    const dq = Math.round(0.5 * dqBase + 0.25 * dwellCoveragePct + 0.25 * processCoveragePct);
    const badge = scoreToBadge(dq);

    const dwellSeries = quantileSeriesFromMap(this.dwellByMonth);
    const processSeries = quantileSeriesFromMap(this.processByMonth);

    const requesterTop = this.moveRequestedBy.top(8);

    return {
      report: 'dockdoor_history',
      meta,
      dataQuality: { score: dq, ...badge, parseOk: this.parseOk, parseFails: this.parseFails, totalRows: this.totalRows, dwellCoveragePct, processCoveragePct },
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
          id: 'dwell_process_medians_monthly',
          title: 'Median dwell & process times (monthly)',
          kind: 'line',
          description: 'Median minutes per month (streaming quantile estimation).',
          data: {
            labels: unionSorted(dwellSeries.labels, processSeries.labels),
            datasets: [
              { label: 'Dwell median (min)', data: alignSeries(unionSorted(dwellSeries.labels, processSeries.labels), dwellSeries.labels, dwellSeries.median) },
              { label: 'Process median (min)', data: alignSeries(unionSorted(dwellSeries.labels, processSeries.labels), processSeries.labels, processSeries.median) },
            ]
          },
          csv: {
            columns: ['month', 'dwell_median_min', 'process_median_min', 'timezone'],
            rows: unionSorted(dwellSeries.labels, processSeries.labels).map((m, i) => ({
              month: m,
              dwell_median_min: alignSeries(unionSorted(dwellSeries.labels, processSeries.labels), dwellSeries.labels, dwellSeries.median)[i],
              process_median_min: alignSeries(unionSorted(dwellSeries.labels, processSeries.labels), processSeries.labels, processSeries.median)[i],
              timezone: meta.timezone
            }))
          }
        },
        {
          id: 'top_move_requested_by',
          title: 'Top move_requested_by counts',
          kind: 'bar',
          description: 'Helps infer module adoption (admin vs others).',
          data: {
            labels: requesterTop.map(x => x.key),
            datasets: [{ label: 'Requests', data: requesterTop.map(x => x.value) }]
          },
          csv: {
            columns: ['move_requested_by', 'count'],
            rows: requesterTop.map(x => ({ move_requested_by: x.key, count: x.value }))
          }
        }
      ],
      findings,
      recommendations: recs,
      roi: computeLaborROIIfEnabled({
        meta,
        // crude proxy: if median dwell decreased? MVP: not enough to compute delta, so skip
        metrics: { note: 'Dock door ROI model not included in MVP draft.' },
        assumptions: meta.assumptions
      }),
    };
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
  }

  getDistinct(map, key) {
    if (!map.has(key)) map.set(key, new ApproxDistinct(2048));
    return map.get(key);
  }

  ingest({ row }) {
    this.totalRows++;
    this.movesTotal++;

    const driver = safeStr(firstPresent(row, ['driver_name', 'driver', 'driver_username', 'driver_id']));
    if (driver) this.movesByDriver.inc(driver);

    // Determine event time for grouping (complete time preferred)
    const complete = parseTimestamp(firstPresent(row, ['complete_time', 'move_complete_time', 'completed_at', 'complete_timestamp']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    const start = parseTimestamp(firstPresent(row, ['start_time', 'move_start_time', 'started_at']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    const accept = parseTimestamp(firstPresent(row, ['accept_time', 'move_accept_time', 'accepted_at']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });

    const eventDt = complete || start || accept;
    if (eventDt) this.parseOk++;

    if (eventDt) {
      const wk = weekKey(eventDt, this.timezone);
      const dy = dayKey(eventDt, this.timezone);
      this.movesByWeek.inc(wk);
      this.movesByDay.inc(dy);
      if (driver) {
        this.getDistinct(this.activeDriversByWeek, wk).add(driver);
        this.getDistinct(this.activeDriversByDay, dy).add(driver);
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

    const weekLabels = Array.from(this.activeDriversByWeek.keys()).sort();
    const movesWeek = weekLabels.map(w => this.movesByWeek.map.get(w) || 0);
    const activeWeek = weekLabels.map(w => this.activeDriversByWeek.get(w)?.estimate() || 0);

    const topDrivers = this.movesByDriver.top(10);

    const findings = [];
    const recs = [];

    if (compliancePct !== null) {
      if (compliancePct < 30) {
        findings.push({ level: 'yellow', text: `Compliance signal is low (${compliancePct}% within ≤2 minutes). Timing-based KPIs may be low-confidence.`, confidence: 'medium' });
        recs.push('Recommend retraining on driver workflow (accept/start/complete), and validate device connectivity + timestamp capture.');
      } else {
        findings.push({ level: 'green', text: `Compliance signal: ${compliancePct}% within ≤2 minutes.`, confidence: 'medium' });
      }
    }

    const queueMed = this.queueMedian.value();
    const queueP90 = this.queueP90.value();
    if (Number.isFinite(queueMed) && queueMed > 10) {
      findings.push({ level: 'yellow', text: `Median queue time is ~${Math.round(queueMed)} minutes (p90 ~${Math.round(queueP90 || 0)}).`, confidence: 'medium' });
      recs.push('Investigate bottlenecks (gate, dispatch, dock availability). Queue time is a classic “hidden tax.”');
    }

    const dqBase = this.dataQualityScore();
    const dq = Math.round(0.55 * dqBase + 0.45 * (this.complianceTotal ? 100 : 70));
    const badge = scoreToBadge(dq);

    return {
      report: 'driver_history',
      meta,
      dataQuality: { score: dq, ...badge, parseOk: this.parseOk, parseFails: this.parseFails, totalRows: this.totalRows },
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
          id: 'active_drivers_and_moves_by_week',
          title: 'Active drivers & moves (weekly)',
          kind: 'line',
          description: 'Weekly trend using approximate distinct counting (no raw driver lists stored).',
          data: {
            labels: weekLabels,
            datasets: [
              { label: 'Active drivers (approx)', data: activeWeek },
              { label: 'Moves', data: movesWeek }
            ]
          },
          csv: {
            columns: ['week', 'active_drivers_approx', 'moves', 'timezone'],
            rows: weekLabels.map((w, i) => ({
              week: w,
              active_drivers_approx: activeWeek[i],
              moves: movesWeek[i],
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

    this.byWeek = new CounterMap();
    this.byMonth = new CounterMap();
  }

  ingest({ row }) {
    this.totalRows++;

    const event = safeStr(firstPresent(row, ['event', 'event_type', 'event_name', 'event_string', 'action', 'status_change']));
    if (event) this.eventTypes.inc(event);

    const dt = parseTimestamp(firstPresent(row, ['event_time', 'created_at', 'timestamp', 'event_timestamp']), {
      timezone: this.timezone, assumeUTC: true, onFail: () => { this.parseFails++; }
    });
    if (dt) this.parseOk++;

    const carrier = row.scac ?? row.carrier_scac ?? row.scac_code ?? row.carrier;

    const isLost = /marked\s+lost|trailer\s+marked\s+lost|\blost\b/i.test(event);
    if (isLost) {
      this.lostCount++;
      if (!isNil(carrier)) this.lostByCarrier.inc(carrier);
      if (dt) {
        this.byWeek.inc(weekKey(dt, this.timezone));
        this.byMonth.inc(monthKey(dt, this.timezone));
      }
    }
  }

  finalize(meta) {
    const findings = [];
    const recs = [];

    if (this.lostCount === 0) {
      findings.push({ level: 'green', text: 'No “Trailer marked lost” events found in the selected range.', confidence: 'medium' });
      recs.push('If lost events are expected but missing, confirm event strings and report configuration match local processes.');
    } else if (this.lostCount > 10) {
      findings.push({ level: 'yellow', text: `Detected ${this.lostCount} “Trailer marked lost” events. This is a classic chaos signal.`, confidence: 'high' });
      recs.push('Investigate top carriers and process handoffs causing location drift; tighten scan/check-in and yard check frequency.');
    }

    const dq = this.dataQualityScore();
    const badge = scoreToBadge(dq);

    const topCarriers = this.lostByCarrier.top(8);
    const topEvents = this.eventTypes.top(10);

    // Pick week vs month series (simple heuristic)
    const rangeDays = roughRangeDays(meta.startDate, meta.endDate);
    const useWeek = rangeDays <= 120;

    const series = useWeek ? counterToSeries(this.byWeek) : counterToSeries(this.byMonth);
    const seriesLabel = useWeek ? 'week' : 'month';

    return {
      report: 'trailer_history',
      meta,
      dataQuality: { score: dq, ...badge, parseOk: this.parseOk, parseFails: this.parseFails, totalRows: this.totalRows },
      metrics: {
        lost_events_count: this.lostCount,
        top_carriers_for_lost: topCarriers,
      },
      charts: [
        {
          id: 'lost_events_over_time',
          title: `Lost events per ${seriesLabel}`,
          kind: 'line',
          description: `Counts grouped by ${seriesLabel} (timezone-adjusted grouping).`,
          data: {
            labels: series.labels,
            datasets: [{ label: 'Lost events', data: series.values }]
          },
          csv: {
            columns: [seriesLabel, 'lost_events', 'timezone'],
            rows: series.labels.map((k, i) => ({ [seriesLabel]: k, lost_events: series.values[i], timezone: meta.timezone }))
          }
        },
        {
          id: 'top_carriers_lost_events',
          title: 'Top carriers by lost events',
          kind: 'bar',
          description: 'Carriers most associated with “lost” events.',
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
      roi: null,
      extras: {
        event_type_top10: topEvents
      }
    };
  }
}

// ---------- Factory ----------
export function createAnalyzers({ timezone, startDate, endDate, assumptions, onWarning }) {
  const base = { timezone, startDate, endDate, assumptions, onWarning };
  return {
    current_inventory: new CurrentInventoryAnalyzer(base),
    detention_history: new DetentionHistoryAnalyzer(base),
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

function roughRangeDays(startDate, endDate) {
  const DateTime = getDateTime();
  if (!startDate || !endDate) return 999;
  const a = DateTime.fromISO(startDate, { zone: 'utc' });
  const b = DateTime.fromISO(endDate, { zone: 'utc' });
  if (!a.isValid || !b.isValid) return 999;
  return Math.max(0, Math.round(b.diff(a, 'days').days));
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
  const enabled =
    Number.isFinite(a.detention_cost_per_hour) &&
    Number.isFinite(a.labor_fully_loaded_rate_per_hour) &&
    Number.isFinite(a.target_moves_per_driver_per_day);

  if (!enabled) return null;

  // MVP: very light-touch labor ROI proxy:
  // If avg moves/driver/day is below target, show “capacity gap” value per day.
  const avg = metrics.avgMovesPerDriverPerDay;
  if (!Number.isFinite(avg)) return {
    label: 'Labor efficiency estimate',
    assumptionsUsed: {
      labor_fully_loaded_rate_per_hour: a.labor_fully_loaded_rate_per_hour,
      target_moves_per_driver_per_day: a.target_moves_per_driver_per_day,
    },
    estimate: null,
    disclaimer: 'Insufficient data to estimate labor impact (missing driver/day aggregation).',
  };

  const target = a.target_moves_per_driver_per_day;
  const gap = Math.max(0, target - avg);

  // Assume 1 driver-day “cost” ~ 8 hours labor (placeholder). This is a *big* assumption.
  const estHours = (gap / target) * 8;
  const estValue = estHours * a.labor_fully_loaded_rate_per_hour;

  return {
    label: 'Labor capacity estimate',
    assumptionsUsed: {
      labor_fully_loaded_rate_per_hour: a.labor_fully_loaded_rate_per_hour,
      target_moves_per_driver_per_day: target,
      driver_day_hours: 8,
    },
    estimate: {
      avg_moves_per_driver_per_day: avg,
      target_moves_per_driver_per_day: target,
      estimated_hours_gap_per_driver_day: round1(estHours),
      estimated_value_per_driver_day: Math.round(estValue * 100) / 100,
    },
    disclaimer: 'Estimate only. Uses a simplified “capacity gap” model. Replace with site-specific staffing and shift assumptions.',
  };
}
