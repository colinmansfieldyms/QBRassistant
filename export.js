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

export function buildSummaryTxt({ inputs, results, warnings, aiInsights, isMultiFacility, detectedFacilities, viewMode = 'all_facilities', activeFacilities = [] }) {
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
  if (viewMode === 'campus') {
    lines.push(`View:        Campus Operation`);
    lines.push(`Campus:      ${activeFacilities.join(', ')}`);
  } else if (viewMode === 'per_facility') {
    lines.push(`View:        Per-Facility`);
    lines.push(`Facilities:  ${activeFacilities.join(', ')}`);
  } else if (isMultiFacility && detectedFacilities?.length > 1) {
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

  function collectFromResult(report, res, facilityLabel) {
    const prefix = facilityLabel ? `${facilityLabel} — ` : '';
    if (res.findings?.length) {
      for (const f of res.findings) {
        allFindings.push({ report, ...f, text: prefix + f.text });
      }
    }
    if (res.recommendations?.length) {
      for (const r of res.recommendations) {
        allRecommendations.push({ report, text: prefix + r });
      }
    }
    if (res.roi?.insights?.length) {
      for (const insight of res.roi.insights) {
        allROIInsights.push({ report, text: prefix + insight });
      }
    }
  }

  for (const [report, res] of Object.entries(results)) {
    if (viewMode === 'per_facility' && res.byFacility) {
      for (const [fac, facResult] of Object.entries(res.byFacility)) {
        collectFromResult(report, facResult, fac);
      }
    } else {
      collectFromResult(report, res, null);
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

  function renderResultDetail(res, indent = '  ') {
    lines.push(`${indent}Data Quality: ${res.dataQuality?.score ?? '—'}/100 (${res.dataQuality?.label ?? '—'})`);
    lines.push(`${indent}Rows Processed: ${(res.dataQuality?.totalRows ?? 0).toLocaleString()}`);
    lines.push('');

    if (res.metrics && Object.keys(res.metrics).length) {
      lines.push(`${indent}KEY METRICS:`);
      for (const [k, v] of Object.entries(res.metrics)) {
        if (typeof v === 'object' && v !== null && !Array.isArray(v)) continue;
        const formattedKey = formatMetricKey(k);
        lines.push(`${indent}  • ${formattedKey}: ${formatValue(v)}`);
      }
      lines.push('');
    }

    if (res.roi?.estimate) {
      lines.push(`${indent}ROI ESTIMATES:`);
      lines.push(`${indent}  Label: ${res.roi.label || 'N/A'}`);
      for (const [k, v] of Object.entries(res.roi.estimate)) {
        if (v !== null && v !== undefined) {
          const formattedKey = formatMetricKey(k);
          lines.push(`${indent}  • ${formattedKey}: ${formatValue(v)}`);
        }
      }
      if (res.roi.disclaimer) {
        lines.push('');
        lines.push(`${indent}  Note: ${wrapText(res.roi.disclaimer, LINE_WIDTH - 10, indent + '        ')}`);
      }
      lines.push('');
    }
  }

  for (const [report, res] of Object.entries(results)) {
    lines.push(`▶ ${formatReportName(report)}`);
    lines.push(SECTION_DIVIDER);
    lines.push('');

    if (viewMode === 'per_facility' && res.byFacility) {
      for (const [fac, facResult] of Object.entries(res.byFacility)) {
        lines.push(`  ┌─ ${fac}`);
        renderResultDetail(facResult, '  │ ');
        lines.push('');
      }
    } else {
      renderResultDetail(res);
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

/**
 * Build a comprehensive JSON export designed for AI consumption.
 * Includes all chart data, findings, recommendations, ROI, and sufficient context
 * for an AI with minimal prior knowledge to extract meaningful insights.
 */
export function buildExportJson({ inputs, results, warnings, isMultiFacility, detectedFacilities, viewMode = 'all_facilities', activeFacilities = [] }) {
  const REPORT_DESCRIPTIONS = {
    current_inventory: 'Snapshot of all trailers currently in the yard, their statuses, move types, age, and carrier assignments. Used to assess yard congestion, inventory health, and data quality.',
    detention_history: 'Historical record of trailer detention events — when trailers exceeded free time and incurred carrier charges. Tracks prevention rates, detention costs, and carrier performance.',
    dockdoor_history: 'Dock door utilization data including dwell times (total time at door), process times (active loading/unloading), and door turn counts. Measures dock efficiency and throughput.',
    driver_history: 'Yard driver (jockey) activity logs including move completions, queue wait times, compliance rates, and deadhead (empty driving) ratios. Measures labor efficiency.',
    trailer_history: 'Trailer event log tracking error-indicating events such as trailers marked lost, yard check inserts, spot edits, and facility edits. Used to assess operational accuracy and chaos.',
  };

  const METRIC_GLOSSARY = {
    total_trailers: 'Count of trailers currently in the yard',
    stale_30d_pct: 'Percentage of trailers that have been in the yard for over 30 days',
    placeholder_scac_pct: 'Percentage of trailers with placeholder/unknown carrier SCAC codes',
    outbound_vs_inbound_ratio: 'Ratio of outbound to inbound trailers (1.0 = balanced)',
    live_load_missing_driver_contact_pct: 'Percentage of live loads missing driver contact info',
    pre_detention_count: 'Number of trailers approaching detention threshold',
    detention_count: 'Number of trailers that exceeded detention threshold and incurred charges',
    prevented_detention_count: 'Number of trailers moved before detention threshold',
    prevention_rate: 'Percentage of at-risk trailers that were moved before detention (higher is better)',
    live_load_count: 'Detention events involving live (driver-attended) loads',
    drop_load_count: 'Detention events involving drop (unattended) loads',
    median_dwell_time_min: 'Median total time (minutes) a trailer spends at a dock door',
    median_process_time_min: 'Median active processing time (minutes) at a dock door',
    avg_turns_per_door_per_day: 'Average number of trailers served per door per day (throughput)',
    process_adoption_pct: 'Percentage of dock events with process time data captured',
    unique_doors: 'Number of distinct dock doors used',
    total_turns: 'Total door turn count across all doors',
    moves_total: 'Total completed yard moves (driver assignments)',
    compliance_pct: 'Percentage of moves completed in compliance with procedures',
    queue_median_minutes: 'Median time (minutes) a move request waits before driver accepts',
    queue_p90_minutes: '90th percentile queue wait time — 10% of moves waited longer than this',
    deadhead_median_minutes: 'Median time (minutes) driver spends driving empty to pickup',
    execution_median_minutes: 'Median time (minutes) to complete a move once started',
    deadhead_ratio_pct: 'Percentage of driver time spent on empty (deadhead) travel',
    avg_moves_per_driver_per_day: 'Average moves completed per driver per day (productivity)',
    total_error_events: 'Total error-indicating events (lost trailers, yard check inserts, spot/facility edits)',
    trailer_marked_lost: 'Count of "trailer marked lost" events — highest-severity error',
    yard_check_insert: 'Count of yard check insert events — trailer found in unexpected location',
    spot_edited: 'Count of spot edit events — trailer location corrected manually',
    facility_edited: 'Count of facility edit events — trailer facility assignment corrected',
    errors_per_day: 'Average error events per day across the reporting period',
    error_rate_pct: 'Error events as a percentage of total events processed',
  };

  const now = DateTime.now().setZone(inputs.timezone).toFormat('yyyy-LL-dd HH:mm:ss ZZZZ');

  function serializeResult(res) {
    const charts = (res.charts || []).map(c => ({
      id: c.id,
      title: c.title,
      kind: c.kind,
      description: c.description || null,
      data: c.data ? {
        labels: c.data.labels || [],
        datasets: (c.data.datasets || []).map(ds => ({
          label: ds.label,
          data: ds.data,
        })),
      } : null,
    }));

    const annotatedMetrics = {};
    for (const [k, v] of Object.entries(res.metrics || {})) {
      if (k.startsWith('_debug')) continue;
      annotatedMetrics[k] = {
        value: v,
        description: METRIC_GLOSSARY[k] || null,
      };
    }

    return {
      dataQuality: {
        score: res.dataQuality?.score ?? null,
        label: res.dataQuality?.label ?? null,
        totalRows: res.dataQuality?.totalRows ?? 0,
      },
      metrics: annotatedMetrics,
      charts,
      findings: (res.findings || []).map(f => ({
        severity: f.level || 'info',
        text: f.text,
        confidence: f.confidence || null,
      })),
      recommendations: res.recommendations || [],
      roi: res.roi ? {
        label: res.roi.label || null,
        estimate: res.roi.estimate || null,
        insights: res.roi.insights || [],
        disclaimer: res.roi.disclaimer || null,
      } : null,
      topEventStrings: res.extras?.event_type_top10?.map(x => ({ event: x.key, count: x.value })) || null,
    };
  }

  const reportSections = {};

  if (viewMode === 'per_facility') {
    // Per-facility mode: each report has byFacility with individual results
    for (const [report, wrapper] of Object.entries(results)) {
      const facilityData = {};
      if (wrapper.byFacility) {
        for (const [fac, res] of Object.entries(wrapper.byFacility)) {
          facilityData[fac] = serializeResult(res);
        }
      }
      reportSections[report] = {
        description: REPORT_DESCRIPTIONS[report] || null,
        byFacility: facilityData,
      };
    }
  } else {
    // all_facilities or campus mode: flat results per report
    for (const [report, res] of Object.entries(results)) {
      reportSections[report] = {
        description: REPORT_DESCRIPTIONS[report] || null,
        ...serializeResult(res),
      };
    }
  }

  // Build view description for AI consumers
  let viewDescription;
  if (viewMode === 'campus') {
    viewDescription = `Campus view — aggregated results combining facilities: ${activeFacilities.join(', ')}. Metrics, findings, and recommendations reflect the combined operation of these facilities as a single campus.`;
  } else if (viewMode === 'per_facility') {
    viewDescription = `Per-facility view — individual results for each selected facility: ${activeFacilities.join(', ')}. Each report contains a "byFacility" object with separate analysis per facility.`;
  } else {
    viewDescription = 'All-facilities view — aggregated results across all detected facilities.';
  }

  return JSON.stringify({
    _meta: {
      description: 'YardIQ yard management analysis export. Each report section contains metrics, chart data, findings, recommendations, and ROI estimates for a specific aspect of yard operations.',
      generated: now,
      version: '1.0',
      findingSeverityScale: 'green (healthy) → yellow (warning) → red (critical)',
      roiDisclaimer: 'ROI estimates are directional projections based on configurable assumptions, not guarantees.',
    },
    context: {
      tenant: inputs.tenant || null,
      facilities: inputs.facilities || [],
      dateRange: { start: inputs.startDate, end: inputs.endDate },
      timezone: inputs.timezone,
      isMultiFacility,
      detectedFacilities: detectedFacilities || [],
      viewMode,
      viewDescription,
      activeFacilities: activeFacilities || [],
      assumptions: inputs.assumptions || {},
    },
    reports: reportSections,
    warnings: warnings || [],
  }, null, 2);
}
