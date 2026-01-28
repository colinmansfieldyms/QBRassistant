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

export function buildSummaryTxt({ inputs, results, warnings, aiInsights, isMultiFacility, detectedFacilities }) {
  const now = DateTime.now().setZone(inputs.timezone).toFormat('yyyy-LL-dd HH:mm:ss ZZZZ');
  const LINE_WIDTH = 64;
  const DIVIDER = '═'.repeat(LINE_WIDTH);
  const SECTION_DIVIDER = '─'.repeat(LINE_WIDTH);

  const lines = [];

  // ═══════════════════════════════════════════════════════════════
  // HEADER
  // ═══════════════════════════════════════════════════════════════
  lines.push(DIVIDER);
  lines.push(centerText('YARDIQ ANALYSIS REPORT', LINE_WIDTH));
  lines.push(DIVIDER);
  lines.push('');
  lines.push(`Generated:   ${now}`);
  lines.push(`Tenant:      ${inputs.tenant || 'N/A'}`);
  lines.push(`Facilities:  ${(inputs.facilities || []).join(', ') || 'N/A'}`);
  lines.push(`Date Range:  ${inputs.startDate} → ${inputs.endDate}`);
  lines.push(`Timezone:    ${inputs.timezone}`);
  if (isMultiFacility && detectedFacilities?.length > 1) {
    lines.push(`Analysis:    Multi-facility (${detectedFacilities.length} facilities detected)`);
  }
  lines.push('');

  // ═══════════════════════════════════════════════════════════════
  // AI INSIGHTS (if generated)
  // ═══════════════════════════════════════════════════════════════
  if (aiInsights && (aiInsights.insights?.length || aiInsights.summary)) {
    lines.push(DIVIDER);
    lines.push(centerText('✨ YARDIQ AI INSIGHTS', LINE_WIDTH));
    lines.push(DIVIDER);
    lines.push('');

    if (aiInsights.insights?.length) {
      lines.push('TOP INSIGHTS:');
      lines.push('');
      aiInsights.insights.forEach((insight, i) => {
        lines.push(`  ${i + 1}. ${wrapText(insight, LINE_WIDTH - 5, '     ')}`);
        lines.push('');
      });
    }

    if (aiInsights.summary) {
      lines.push('SUMMARY:');
      lines.push('');
      lines.push(wrapText(aiInsights.summary, LINE_WIDTH, '  '));
      lines.push('');
    }
    lines.push('');
  }

  // ═══════════════════════════════════════════════════════════════
  // EXECUTIVE SUMMARY - All Findings & Recommendations
  // ═══════════════════════════════════════════════════════════════
  const allFindings = [];
  const allRecommendations = [];
  const allROIInsights = [];

  for (const [report, res] of Object.entries(results)) {
    if (res.findings?.length) {
      for (const f of res.findings) {
        allFindings.push({ report, ...f });
      }
    }
    if (res.recommendations?.length) {
      for (const r of res.recommendations) {
        allRecommendations.push({ report, text: r });
      }
    }
    if (res.roi?.insights?.length) {
      for (const insight of res.roi.insights) {
        allROIInsights.push({ report, text: insight });
      }
    }
  }

  lines.push(DIVIDER);
  lines.push(centerText('EXECUTIVE SUMMARY', LINE_WIDTH));
  lines.push(DIVIDER);
  lines.push('');

  // Findings grouped by severity
  const criticalFindings = allFindings.filter(f => f.level === 'critical');
  const warningFindings = allFindings.filter(f => f.level === 'warning');
  const infoFindings = allFindings.filter(f => f.level === 'info' || !f.level);

  if (criticalFindings.length) {
    lines.push('⚠️  CRITICAL FINDINGS:');
    lines.push('');
    for (const f of criticalFindings) {
      lines.push(`    • [${f.report}] ${wrapText(f.text, LINE_WIDTH - 6, '      ')}`);
    }
    lines.push('');
  }

  if (warningFindings.length) {
    lines.push('⚡ WARNINGS:');
    lines.push('');
    for (const f of warningFindings) {
      lines.push(`    • [${f.report}] ${wrapText(f.text, LINE_WIDTH - 6, '      ')}`);
    }
    lines.push('');
  }

  if (infoFindings.length) {
    lines.push('ℹ️  INSIGHTS:');
    lines.push('');
    for (const f of infoFindings) {
      lines.push(`    • [${f.report}] ${wrapText(f.text, LINE_WIDTH - 6, '      ')}`);
    }
    lines.push('');
  }

  if (!allFindings.length) {
    lines.push('No significant findings detected.');
    lines.push('');
  }

  // Recommendations
  lines.push(SECTION_DIVIDER);
  lines.push('');
  lines.push('📋 RECOMMENDATIONS:');
  lines.push('');
  if (allRecommendations.length) {
    for (const r of allRecommendations) {
      lines.push(`    • ${wrapText(r.text, LINE_WIDTH - 6, '      ')}`);
    }
  } else {
    lines.push('    No recommendations at this time.');
  }
  lines.push('');

  // ROI Insights
  if (allROIInsights.length) {
    lines.push(SECTION_DIVIDER);
    lines.push('');
    lines.push('💰 ROI INSIGHTS:');
    lines.push('');
    for (const r of allROIInsights) {
      lines.push(`    • ${wrapText(r.text, LINE_WIDTH - 6, '      ')}`);
    }
    lines.push('');
  }

  // ═══════════════════════════════════════════════════════════════
  // DETAILED REPORT BREAKDOWN
  // ═══════════════════════════════════════════════════════════════
  lines.push(DIVIDER);
  lines.push(centerText('DETAILED REPORT BREAKDOWN', LINE_WIDTH));
  lines.push(DIVIDER);
  lines.push('');

  for (const [report, res] of Object.entries(results)) {
    lines.push(`▶ ${formatReportName(report)}`);
    lines.push(SECTION_DIVIDER);
    lines.push('');

    // Data quality
    lines.push(`  Data Quality: ${res.dataQuality?.score ?? '—'}/100 (${res.dataQuality?.label ?? '—'})`);
    lines.push(`  Rows Processed: ${(res.dataQuality?.totalRows ?? 0).toLocaleString()}`);
    lines.push('');

    // Key metrics (formatted nicely)
    if (res.metrics && Object.keys(res.metrics).length) {
      lines.push('  KEY METRICS:');
      for (const [k, v] of Object.entries(res.metrics)) {
        // Skip complex objects in the summary
        if (typeof v === 'object' && v !== null && !Array.isArray(v)) continue;
        const formattedKey = formatMetricKey(k);
        lines.push(`    • ${formattedKey}: ${formatValue(v)}`);
      }
      lines.push('');
    }

    // ROI estimates
    if (res.roi?.estimate) {
      lines.push('  ROI ESTIMATES:');
      lines.push(`    Label: ${res.roi.label || 'N/A'}`);
      for (const [k, v] of Object.entries(res.roi.estimate)) {
        if (v !== null && v !== undefined) {
          const formattedKey = formatMetricKey(k);
          lines.push(`    • ${formattedKey}: ${formatValue(v)}`);
        }
      }
      if (res.roi.disclaimer) {
        lines.push('');
        lines.push(`    Note: ${wrapText(res.roi.disclaimer, LINE_WIDTH - 10, '          ')}`);
      }
      lines.push('');
    }

    lines.push('');
  }

  // ═══════════════════════════════════════════════════════════════
  // ASSUMPTIONS USED
  // ═══════════════════════════════════════════════════════════════
  lines.push(DIVIDER);
  lines.push(centerText('ASSUMPTIONS USED', LINE_WIDTH));
  lines.push(DIVIDER);
  lines.push('');
  const a = inputs.assumptions || {};
  lines.push(`  Detention Cost ($/hr):           ${a.detention_cost_per_hour ?? '(not set)'}`);
  lines.push(`  Labor Rate ($/hr, fully loaded): ${a.labor_fully_loaded_rate_per_hour ?? '(not set)'}`);
  lines.push(`  Target Moves/Driver/Day:         ${a.target_moves_per_driver_per_day ?? 50}`);
  lines.push(`  Target Turns/Door/Day:           ${a.target_turns_per_door_per_day ?? '(not set)'}`);
  lines.push(`  Cost per Dock Door Hour:         ${a.cost_per_dock_door_hour ?? '(not set)'}`);
  lines.push('');

  // ═══════════════════════════════════════════════════════════════
  // FOOTER
  // ═══════════════════════════════════════════════════════════════
  lines.push(DIVIDER);
  lines.push('');
  lines.push(centerText('Generated by YMS YardIQ', LINE_WIDTH));
  lines.push(centerText('https://yardmanagementsolutions.com', LINE_WIDTH));
  lines.push(DIVIDER);

  return lines.join('\n');
}

// Helper: Center text within a given width
function centerText(text, width) {
  const padding = Math.max(0, Math.floor((width - text.length) / 2));
  return ' '.repeat(padding) + text;
}

// Helper: Wrap long text with continuation indent
function wrapText(text, maxWidth, indent = '') {
  if (!text) return '';
  const words = text.split(' ');
  const lines = [];
  let currentLine = '';

  for (const word of words) {
    if (currentLine.length + word.length + 1 <= maxWidth) {
      currentLine += (currentLine ? ' ' : '') + word;
    } else {
      if (currentLine) lines.push(currentLine);
      currentLine = word;
    }
  }
  if (currentLine) lines.push(currentLine);

  return lines.join('\n' + indent);
}

// Helper: Format report names nicely
function formatReportName(report) {
  return report
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

// Helper: Format metric keys nicely
function formatMetricKey(key) {
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/Pct/g, '%')
    .replace(/Avg/g, 'Average');
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
