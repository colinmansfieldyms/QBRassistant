/**
 * TXT generation + print view + CSV helpers
 * (PII policy: by the time exports run, PII fields are already scrubbed in analysis.js.)
 */

const { DateTime } = window.luxon;

export function downloadText(filename, text) {
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function downloadCsv(filename, csvText) {
  const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function printReport() {
  window.print();
}

export function buildSummaryTxt({ inputs, results, warnings }) {
  const now = DateTime.now().setZone(inputs.timezone).toFormat('yyyy-LL-dd HH:mm:ss ZZZZ');

  const lines = [];
  lines.push('YMS Customer Value Assessment — Summary');
  lines.push('='.repeat(48));
  lines.push(`Generated: ${now}`);
  lines.push(`Tenant: ${inputs.tenant}`);
  lines.push(`Facilities: ${inputs.facilities.join(', ')}`);
  lines.push(`Date range: ${inputs.startDate} → ${inputs.endDate}`);
  lines.push(`Timezone: ${inputs.timezone}`);
  lines.push(`Mode: ${inputs.mockMode ? 'MOCK (no API calls)' : 'LIVE API'}`);
  lines.push('');

  lines.push('Assumptions (ROI)');
  lines.push('-'.repeat(48));
  const a = inputs.assumptions || {};
  lines.push(`detention_cost_per_hour: ${a.detention_cost_per_hour ?? '(not set)'}`);
  lines.push(`labor_fully_loaded_rate_per_hour: ${a.labor_fully_loaded_rate_per_hour ?? '(not set)'}`);
  lines.push(`target_moves_per_driver_per_day: ${a.target_moves_per_driver_per_day ?? 50}`);
  lines.push('');

  for (const [report, res] of Object.entries(results)) {
    lines.push(report);
    lines.push('-'.repeat(48));
    lines.push(`Data quality score: ${res.dataQuality?.score ?? '—'} (${res.dataQuality?.label ?? '—'})`);
    lines.push(`Rows processed: ${res.dataQuality?.totalRows ?? 0}`);
    lines.push('');

    lines.push('Key metrics:');
    for (const [k, v] of Object.entries(res.metrics || {})) {
      lines.push(`- ${k}: ${formatValue(v)}`);
    }
    lines.push('');

    lines.push('Findings:');
    if ((res.findings || []).length) {
      for (const f of res.findings) {
        lines.push(`- [${(f.level || '').toUpperCase()}] (${f.confidence || 'medium'}) ${f.text}`);
      }
    } else {
      lines.push('- (none)');
    }
    lines.push('');

    lines.push('Recommendations:');
    if ((res.recommendations || []).length) {
      for (const r of res.recommendations) lines.push(`- ${r}`);
    } else {
      lines.push('- (none)');
    }
    lines.push('');

    lines.push('ROI (estimates):');
    if (res.roi) {
      lines.push(`- ${res.roi.label}`);
      lines.push(`- Disclaimer: ${res.roi.disclaimer}`);
      lines.push(`- Assumptions used: ${JSON.stringify(res.roi.assumptionsUsed || {}, null, 2)}`);
      lines.push(`- Estimate: ${JSON.stringify(res.roi.estimate || {}, null, 2)}`);
    } else {
      lines.push('- (not enabled / insufficient data)');
    }
    lines.push('');
  }

  lines.push('Warnings');
  lines.push('-'.repeat(48));
  if (warnings && warnings.length) lines.push(...warnings);
  else lines.push('None.');

  lines.push('');
  lines.push('PII Policy');
  lines.push('-'.repeat(48));
  lines.push('Driver cell numbers are never displayed or exported. Any field name containing "cell" or "phone" is dropped during normalization.');
  lines.push('Only presence/absence of driver contact fields may be used as an adoption metric.');

  return lines.join('\n');
}

function formatValue(v) {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : '—';
  if (Array.isArray(v)) return JSON.stringify(v);
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

/**
 * Convenience helper for “Download aggregated report summary CSV” if you want it elsewhere.
 * (In this draft, charts.js builds its own CSV text to keep wiring simple.)
 */
export function buildReportSummaryCsv({ report, result, timezone, startDate, endDate }) {
  const lines = [];
  lines.push(`# report=${report}`);
  lines.push(`# timezone=${timezone}`);
  lines.push(`# date_range=${startDate}..${endDate}`);
  lines.push('metric,value');

  for (const [k, v] of Object.entries(result.metrics || {})) {
    lines.push(`${csvEscape(k)},${csvEscape(v)}`);
  }
  lines.push(`${csvEscape('data_quality_score')},${csvEscape(result.dataQuality?.score ?? '')}`);
  lines.push(`${csvEscape('data_quality_confidence')},${csvEscape(result.dataQuality?.label ?? '')}`);

  return lines.join('\n');
}

/**
 * Convenience helper for chart CSV if you want it elsewhere.
 */
export function buildChartCsv({ report, chartId, timezone, startDate, endDate, columns, rows }) {
  const lines = [];
  lines.push(`# report=${report}`);
  lines.push(`# chart=${chartId}`);
  lines.push(`# timezone=${timezone}`);
  lines.push(`# date_range=${startDate}..${endDate}`);
  lines.push(columns.map(csvEscape).join(','));
  for (const r of rows) lines.push(columns.map(c => csvEscape(r[c])).join(','));
  return lines.join('\n');
}

function csvEscape(v) {
  const s = (v === null || v === undefined) ? '' : String(v);
  if (/[,"\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}
