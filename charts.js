import { downloadText } from './export.js?v=2025.01.07.0';
import { applyPartialPeriodHandling } from './analysis.js?v=2025.01.07.0';

/**
 * Chart.js rendering + export PNG + provide chart datasets for CSV export.
 * NOTE: Chart.js is loaded via CDN and available as window.Chart.
 */

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') node.className = v;
    else if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2).toLowerCase(), v);
    else if (v !== null && v !== undefined) node.setAttribute(k, v);
  }
  for (const c of children) node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  return node;
}

/**
 * Convert snake_case report name to Title Case for display
 * e.g., "driver_history" -> "Driver History"
 */
function humanizeReportName(name) {
  if (!name) return '';
  return name
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}

/**
 * Glossary of terms that should have tooltips in findings/recommendations.
 * Key is the term (case-insensitive match), value is the definition.
 */
const GLOSSARY = {
  'queue time': 'Time a move request waits before a driver accepts it. Calculated from when the move is created until a driver taps "Accept."',
  'deadhead': 'Time drivers spend traveling to a trailer before starting the move. High deadhead % means drivers are often assigned trailers far from their current location.',
  'dwell time': 'Total time a trailer spends at a dock door, from arrival to departure.',
  'process time': 'Time spent actively loading or unloading a trailer at a dock door.',
  'detention': 'Time a trailer waits beyond the allowed free time before loading/unloading begins. Often results in carrier fees.',
  'turns per door': 'Number of trailers processed through a single dock door in a day. Higher turns = better door utilization.',
  'compliance': 'Percentage of moves where drivers properly tap Accept, Start, and Complete in sequence with realistic timing.',
  'lost events': 'Trailers marked as "lost" in the system - their location is unknown. Often due to missed check-out scans.',
};

/**
 * Wraps glossary terms in text with tooltip spans.
 * Returns an array of DOM nodes (text nodes and span elements).
 */
function wrapGlossaryTerms(text) {
  const nodes = [];
  let remaining = text;

  // Build regex to match any glossary term (case-insensitive)
  const terms = Object.keys(GLOSSARY).sort((a, b) => b.length - a.length); // Longest first
  const pattern = new RegExp(`(${terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'gi');

  let lastIndex = 0;
  let match;

  // Reset lastIndex for global regex
  pattern.lastIndex = 0;

  while ((match = pattern.exec(text)) !== null) {
    // Add text before the match
    if (match.index > lastIndex) {
      nodes.push(document.createTextNode(text.slice(lastIndex, match.index)));
    }

    // Add the term with tooltip
    const term = match[1];
    const definition = GLOSSARY[term.toLowerCase()];
    const span = el('span', {
      class: 'glossary-term',
      'data-tooltip': definition,
    }, [term]);
    nodes.push(span);

    lastIndex = pattern.lastIndex;
  }

  // Add remaining text
  if (lastIndex < text.length) {
    nodes.push(document.createTextNode(text.slice(lastIndex)));
  }

  return nodes.length > 0 ? nodes : [document.createTextNode(text)];
}

/**
 * Build tooltip text for confidence badge from dataQuality object.
 * Combines the main tooltip text with any data quality findings.
 */
function buildConfidenceTooltip(dataQuality) {
  if (!dataQuality) return 'Data quality information unavailable';

  const lines = [];

  // Primary tooltip text from analyzer (explains score factors)
  if (dataQuality.tooltipText) {
    lines.push(dataQuality.tooltipText);
  } else {
    // Fallback: generate basic tooltip
    lines.push(`Score: ${dataQuality.score ?? '?'}/100`);
    const total = (dataQuality.parseOk ?? 0) + (dataQuality.parseFails ?? 0);
    if (total > 0) {
      lines.push(`Parse success: ${dataQuality.parseOk}/${total}`);
    }
  }

  // Add data quality findings as bullet points
  if (dataQuality.dataQualityFindings?.length > 0) {
    lines.push('');
    lines.push('Data quality notes:');
    for (const f of dataQuality.dataQualityFindings) {
      const icon = f.level === 'green' ? '✓' : (f.level === 'red' ? '✗' : '⚠');
      lines.push(`${icon} ${f.text}`);
    }
  }

  return lines.join('\n');
}

function chartConfigFromKind(kind, chartData, title, partialPeriodMode = 'include') {
  const partialInfo = chartData?.partialPeriodInfo;
  const isHighlightMode = partialPeriodMode === 'highlight' && partialInfo?.highlightFirst || partialInfo?.highlightLast;

  // Deep clone datasets to avoid mutating original data
  let processedData = chartData;
  if (kind === 'line' && isHighlightMode) {
    processedData = {
      ...chartData,
      datasets: chartData.datasets.map(ds => {
        const newDs = { ...ds };

        // Create segment styling function for dashed lines on partial periods
        if (partialInfo?.highlightFirst || partialInfo?.highlightLast) {
          newDs.segment = {
            borderDash: ctx => {
              const idx = ctx.p0DataIndex;
              const lastIdx = ctx.chart.data.labels.length - 1;
              // Dashed line for segments touching partial periods
              if (partialInfo.highlightFirst && idx === 0) return [6, 3];
              if (partialInfo.highlightLast && idx === lastIdx - 1) return [6, 3];
              return undefined;
            }
          };

          // Create pointStyle function for hollow points on partial periods
          const originalPointRadius = newDs.pointRadius || 4;
          newDs.pointRadius = ctx => {
            const idx = ctx.dataIndex;
            const lastIdx = ctx.chart.data.labels.length - 1;
            if ((partialInfo.highlightFirst && idx === 0) ||
                (partialInfo.highlightLast && idx === lastIdx)) {
              return originalPointRadius + 2; // Slightly larger for partial
            }
            return originalPointRadius;
          };

          // Use hollow points for partial periods
          newDs.pointStyle = ctx => {
            const idx = ctx.dataIndex;
            const lastIdx = ctx.chart.data.labels.length - 1;
            if ((partialInfo.highlightFirst && idx === 0) ||
                (partialInfo.highlightLast && idx === lastIdx)) {
              return 'circle'; // Will be styled differently via pointBackgroundColor
            }
            return 'circle';
          };

          // Hollow background for partial period points
          const originalBgColor = newDs.backgroundColor || newDs.borderColor || '#3b82f6';
          newDs.pointBackgroundColor = ctx => {
            const idx = ctx.dataIndex;
            const lastIdx = ctx.chart.data.labels.length - 1;
            if ((partialInfo.highlightFirst && idx === 0) ||
                (partialInfo.highlightLast && idx === lastIdx)) {
              return 'rgba(255, 255, 255, 0.8)'; // Hollow (white fill)
            }
            return originalBgColor;
          };
        }

        return newDs;
      })
    };
  }

  // Apply outlier styling (orange points for outlier indices)
  if (kind === 'line' && chartData?.datasets?.some(ds => ds.outlierIndices?.length > 0)) {
    processedData = {
      ...processedData,
      datasets: processedData.datasets.map(ds => {
        const outlierIndices = ds.outlierIndices || [];
        if (outlierIndices.length === 0) return ds;

        const newDs = { ...ds };
        const originalBgColor = newDs.backgroundColor || newDs.borderColor || '#3b82f6';

        // Orange background for outlier points
        newDs.pointBackgroundColor = ctx => {
          // If there's already a pointBackgroundColor function (from partial period), compose with it
          const baseColor = typeof ds.pointBackgroundColor === 'function'
            ? ds.pointBackgroundColor(ctx)
            : (ds.pointBackgroundColor || originalBgColor);
          return outlierIndices.includes(ctx.dataIndex) ? '#f97316' : baseColor;
        };

        // Orange border for outlier points
        newDs.pointBorderColor = ctx => {
          const baseBorder = typeof ds.pointBorderColor === 'function'
            ? ds.pointBorderColor(ctx)
            : (ds.pointBorderColor || ds.borderColor || originalBgColor);
          return outlierIndices.includes(ctx.dataIndex) ? '#ea580c' : baseBorder;
        };

        return newDs;
      })
    };
  }

  // Build tooltip callback for outlier indication
  const hasOutliers = chartData?.datasets?.some(ds => ds.outlierIndices?.length > 0);
  const tooltipCallback = hasOutliers ? {
    label: function(context) {
      const ds = context.dataset;
      const outlierIndices = ds.outlierIndices || [];
      const isOutlier = outlierIndices.includes(context.dataIndex);

      let label = ds.label || '';
      if (label) label += ': ';
      if (context.parsed.y != null) {
        label += context.parsed.y.toLocaleString();
      }

      if (isOutlier) {
        label += ' - Outlier';
      }

      return label;
    }
  } : {};

  const base = {
    type: kind === 'pie' ? 'pie' : (kind === 'bar' ? 'bar' : 'line'),
    data: processedData,
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom' },
        title: { display: false, text: title },
        tooltip: { callbacks: tooltipCallback }
      },
      scales: (kind === 'pie') ? {} : {
        x: { ticks: { maxRotation: 0, autoSkip: true } },
        y: { beginAtZero: true }
      }
    }
  };
  return base;
}

export function destroyAllCharts(chartRegistry) {
  for (const [, handles] of chartRegistry.entries()) {
    for (const h of handles) {
      try { h.chart?.destroy(); } catch {}
    }
  }
}

function downloadPngFromCanvas(canvas, filename) {
  const url = canvas.toDataURL('image/png');
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

function createChartModal() {
  const modal = el('div', { class: 'chart-modal' });
  const content = el('div', { class: 'chart-modal-content' });

  const header = el('div', { class: 'chart-modal-header' });
  const title = el('div', { class: 'chart-modal-title' });
  const actions = el('div', { class: 'chart-modal-actions' });

  const downloadBtn = el('button', { class: 'btn btn-primary', type: 'button' }, ['Download PNG']);
  const closeBtn = el('button', { class: 'btn btn-ghost', type: 'button' }, ['Close']);

  actions.appendChild(downloadBtn);
  actions.appendChild(closeBtn);
  header.appendChild(title);
  header.appendChild(actions);

  const body = el('div', { class: 'chart-modal-body' });
  const canvasWrap = el('div', { class: 'chart-modal-canvas-wrap' });
  const canvas = el('canvas');

  canvasWrap.appendChild(canvas);
  body.appendChild(canvasWrap);

  content.appendChild(header);
  content.appendChild(body);
  modal.appendChild(content);

  document.body.appendChild(modal);

  return { modal, title, canvas, downloadBtn, closeBtn, chart: null };
}

let chartModal = null;

function openChartFullscreen(def, chartData, report, onWarning, partialPeriodMode = 'include', enableDrilldown = true) {
  if (!chartModal) {
    chartModal = createChartModal();

    // Close on background click
    chartModal.modal.addEventListener('click', (e) => {
      if (e.target === chartModal.modal) {
        closeChartFullscreen();
      }
    });

    // Close button
    chartModal.closeBtn.addEventListener('click', closeChartFullscreen);

    // ESC key to close
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && chartModal.modal.classList.contains('active')) {
        closeChartFullscreen();
      }
    });
  }

  // Set title
  chartModal.title.textContent = def.title;

  // Destroy previous chart if exists
  if (chartModal.chart) {
    try { chartModal.chart.destroy(); } catch {}
  }

  // Create fullscreen chart with better label visibility
  const cfg = chartConfigFromKind(def.kind, chartData, def.title, partialPeriodMode);

  // Override config for fullscreen to show all labels
  if (cfg.options.scales && cfg.options.scales.x) {
    cfg.options.scales.x.ticks = {
      maxRotation: 45,
      minRotation: 0,
      autoSkip: false, // Show all labels
      font: { size: 11 }
    };
  }

  // Add drilldown click handler if enabled
  const hasDrilldown = enableDrilldown && def.drilldown && def.drilldown.byLabel;
  if (hasDrilldown) {
    cfg.options.onClick = (event, elements, chart) => {
      if (elements.length === 0) return;
      const el = elements[0];
      const label = chart.data.labels?.[el.index];
      if (label && def.drilldown.byLabel[label]) {
        openDrilldownModal(label, def.drilldown.byLabel[label], def.drilldown);
      }
    };
    // Set cursor to pointer for clickable elements
    cfg.options.onHover = (event, elements) => {
      chartModal.canvas.style.cursor = elements.length > 0 ? 'pointer' : 'default';
    };
  }

  chartModal.chart = new window.Chart(chartModal.canvas.getContext('2d'), cfg);

  // Download button
  chartModal.downloadBtn.onclick = () => {
    try {
      downloadPngFromCanvas(chartModal.canvas, `${report}_${def.id}_fullscreen.png`);
    } catch (e) {
      onWarning?.(`PNG export failed: ${e?.message || String(e)}`);
    }
  };

  // Show modal
  chartModal.modal.classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeChartFullscreen() {
  if (!chartModal) return;

  chartModal.modal.classList.remove('active');
  document.body.style.overflow = '';

  // Destroy chart to free memory
  if (chartModal.chart) {
    try { chartModal.chart.destroy(); } catch {}
    chartModal.chart = null;
  }
}

// ---------- Drill-Down Modal ----------
let drilldownModal = null;

function createDrilldownModal() {
  const modal = el('div', { class: 'drilldown-modal' });
  const content = el('div', { class: 'drilldown-modal-content' });

  const header = el('div', { class: 'drilldown-modal-header' });
  const title = el('h3', { class: 'drilldown-modal-title' });
  const closeBtn = el('button', { class: 'drilldown-modal-close', type: 'button' }, ['×']);
  header.appendChild(title);
  header.appendChild(closeBtn);

  const tableWrap = el('div', { class: 'drilldown-table-wrap' });

  const footer = el('div', { class: 'drilldown-modal-footer' });
  const exportBtn = el('button', { class: 'btn', type: 'button' }, ['Export CSV']);
  footer.appendChild(exportBtn);

  content.appendChild(header);
  content.appendChild(tableWrap);
  content.appendChild(footer);
  modal.appendChild(content);

  document.body.appendChild(modal);

  return { modal, title, tableWrap, exportBtn, closeBtn };
}

function openDrilldownModal(label, rows, drilldownConfig) {
  if (!drilldownModal) {
    drilldownModal = createDrilldownModal();

    // Close on background click
    drilldownModal.modal.addEventListener('click', (e) => {
      if (e.target === drilldownModal.modal) {
        closeDrilldownModal();
      }
    });

    // Close button
    drilldownModal.closeBtn.addEventListener('click', closeDrilldownModal);

    // ESC key to close
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && drilldownModal.modal.classList.contains('active')) {
        closeDrilldownModal();
      }
    });
  }

  // Set title
  drilldownModal.title.textContent = `Details for: ${label} (${rows.length} records)`;

  // Build sortable table
  const columns = drilldownConfig.columns || [];
  const columnLabels = drilldownConfig.columnLabels || columns;

  // Track sort state
  let sortColumn = null;
  let sortAscending = true;
  let sortedRows = [...rows];

  function renderTable() {
    drilldownModal.tableWrap.innerHTML = '';

    const table = el('table', { class: 'drilldown-table' });
    const thead = el('thead');
    const headerRow = el('tr');

    columnLabels.forEach((colLabel, idx) => {
      const col = columns[idx];
      const isSorted = sortColumn === col;
      const sortIndicator = isSorted ? (sortAscending ? ' ▲' : ' ▼') : '';
      const th = el('th', {
        class: 'drilldown-th sortable',
        onClick: () => {
          if (sortColumn === col) {
            sortAscending = !sortAscending;
          } else {
            sortColumn = col;
            sortAscending = true;
          }
          // Sort rows
          sortedRows = [...rows].sort((a, b) => {
            const valA = a[col];
            const valB = b[col];
            // Handle numeric vs string
            if (typeof valA === 'number' && typeof valB === 'number') {
              return sortAscending ? valA - valB : valB - valA;
            }
            const strA = String(valA ?? '').toLowerCase();
            const strB = String(valB ?? '').toLowerCase();
            return sortAscending ? strA.localeCompare(strB) : strB.localeCompare(strA);
          });
          renderTable();
        }
      }, [colLabel + sortIndicator]);
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = el('tbody');
    for (const row of sortedRows) {
      const tr = el('tr');
      for (const col of columns) {
        const value = row[col];
        const displayValue = value === '' || value === null || value === undefined ? '—' : String(value);
        tr.appendChild(el('td', {}, [displayValue]));
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);

    drilldownModal.tableWrap.appendChild(table);
  }

  renderTable();

  // Export button
  drilldownModal.exportBtn.onclick = () => {
    // Helper to escape CSV cell values
    const escapeCSV = (val) => {
      if (val === null || val === undefined) return '';
      const str = String(val);
      // Always wrap in quotes and escape internal quotes
      return `"${str.replace(/"/g, '""')}"`;
    };

    const csvLines = [];
    // Escape header row too
    csvLines.push(columnLabels.map(escapeCSV).join(','));
    for (const row of sortedRows) {
      const values = columns.map(col => escapeCSV(row[col]));
      csvLines.push(values.join(','));
    }
    const csvText = csvLines.join('\n');
    const filename = `drilldown_${label.replace(/[^a-z0-9]/gi, '_')}.csv`;
    downloadText(filename, csvText);
  };

  // Show modal
  drilldownModal.modal.classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeDrilldownModal() {
  if (!drilldownModal) return;
  drilldownModal.modal.classList.remove('active');
  document.body.style.overflow = '';
}

export function renderReportResult({
  report,
  result,
  timezone,
  dateRange,
  partialPeriodInfo,
  partialPeriodMode = 'include',
  enableDrilldown = true,
  onWarning,
  chartRegistry,
}) {
  const card = el('div', { class: 'report-card' });

  const badge = el('span', {
    class: `badge badge-tooltip ${result?.dataQuality?.color || 'yellow'}`,
    'data-tooltip': buildConfidenceTooltip(result?.dataQuality)
  }, [
    `${result?.dataQuality?.label || 'Medium'} confidence`
  ]);

  const head = el('div', { class: 'report-head' }, [
    el('div', {}, [
      el('h3', {}, [humanizeReportName(report)]),
      el('div', { class: 'report-sub' }, [
        `Data quality score: `,
        el('b', {}, [String(result?.dataQuality?.score ?? '—')]),
        ` · Rows: ${String(result?.dataQuality?.totalRows ?? 0)} · Parse fails: ${String(result?.dataQuality?.parseFails ?? 0)}`
      ])
    ]),
    badge
  ]);

  const metricsBlock = el('div', {}, [
    el('div', { class: 'section-title' }, [
      el('h2', {}, ['Key metrics']),
      el('button', {
        class: 'btn btn-ghost',
        type: 'button',
        onClick: () => {
          try {
            const csv = buildReportSummaryCsvText(report, result, { timezone, dateRange });
            downloadText(`report_${report}_summary.csv`, csv);
          } catch (e) {
            onWarning?.(`CSV export failed for ${report}: ${e?.message || String(e)}`);
          }
        }
      }, ['⬇ CSV'])
    ]),
    renderMetricsGrid(result.metrics || {})
  ]);

  const findingsBlock = el('div', { style: 'margin-top:16px;' }, [
    renderFindings(result.findings || [], result.recommendations || [], result.roi || null, result.meta || {}, result.detentionSpend || null)
  ]);

  const chartsBlock = el('div', { class: 'chart-grid' });
  const handles = [];

  for (const def of (result.charts || [])) {
    // Apply partial period handling for line charts (time series) FIRST
    // so that actions can use the processed data
    let chartData = def.data;
    if (def.kind === 'line' && partialPeriodInfo?.hasPartialPeriods) {
      const processed = applyPartialPeriodHandling(def.data, partialPeriodInfo, partialPeriodMode);
      chartData = processed;
    }

    const chartCard = el('div', { class: 'chart-card' });
    const canvas = el('canvas', { width: 800, height: 360 });
    const wrap = el('div', { class: 'canvas-wrap', style: 'height:360px;' }, [canvas]);

    // Capture chartData in closure for fullscreen
    const chartDataForFullscreen = chartData;

    // Drilldown indicator - check if this chart has drilldown data
    const hasDrilldown = enableDrilldown && def.drilldown && def.drilldown.byLabel;

    const actionButtons = [];

    // Add drilldown indicator button before other actions
    if (hasDrilldown) {
      actionButtons.push(el('span', {
        class: 'drilldown-badge',
        'data-tooltip': 'Click chart bars or points to view underlying records'
      }, ['🔍 Drill-down']));
    }

    actionButtons.push(
      el('button', {
        class: 'btn btn-ghost',
        type: 'button',
        onClick: () => {
          try {
            openChartFullscreen(def, chartDataForFullscreen, report, onWarning, partialPeriodMode, enableDrilldown);
          } catch (e) {
            onWarning?.(`Fullscreen failed: ${e?.message || String(e)}`);
          }
        }
      }, ['⛶ Expand']),
      el('button', {
        class: 'btn btn-ghost',
        type: 'button',
        onClick: () => {
          try {
            downloadPngFromCanvas(canvas, `${report}_${def.id}.png`);
          } catch (e) {
            onWarning?.(`PNG export failed: ${e?.message || String(e)}`);
          }
        }
      }, ['⬇ PNG']),
      el('button', {
        class: 'btn btn-ghost',
        type: 'button',
        onClick: () => {
          try {
            const csvText = buildChartCsvText(def, result.meta || {}, { timezone, dateRange, report });
            downloadText(`chart_${report}_${def.id}.csv`, csvText);
          } catch (e) {
            onWarning?.(`Chart CSV export failed: ${e?.message || String(e)}`);
          }
        }
      }, ['⬇ CSV'])
    );

    const actions = el('div', { class: 'chart-actions' }, actionButtons);

    const titleContent = [el('b', {}, [def.title])];
    titleContent.push(actions);

    const title = el('div', { class: 'chart-title' }, titleContent);

    const desc = def.description ? el('div', { class: 'muted small', style: 'margin-bottom:8px;' }, [def.description]) : null;

    chartCard.appendChild(title);
    if (desc) chartCard.appendChild(desc);
    chartCard.appendChild(wrap);

    chartsBlock.appendChild(chartCard);

    // Render chart
    const cfg = chartConfigFromKind(def.kind, chartData, def.title, partialPeriodMode);

    // Add onClick handler for drilldown if enabled
    if (hasDrilldown) {
      cfg.options = cfg.options || {};
      cfg.options.onClick = (event, elements, chart) => {
        if (!elements.length) return;

        const element = elements[0];
        const datasetIndex = element.datasetIndex;
        const dataIndex = element.index;
        const label = chart.data.labels[dataIndex];

        // For outlierOnly charts (like dock door), only allow click on outlier points
        if (def.drilldown.outlierOnly) {
          const dataset = chart.data.datasets[datasetIndex];
          if (!dataset.outlierIndices?.includes(dataIndex)) {
            return; // Not an outlier point, ignore click
          }
        }

        const rows = def.drilldown.byLabel[label];
        if (rows && rows.length > 0) {
          openDrilldownModal(label, rows, def.drilldown);
        }
      };

      // Set cursor to pointer for clickable elements
      canvas.style.cursor = 'pointer';
    }

    const chart = new window.Chart(canvas.getContext('2d'), cfg);
    handles.push({ id: def.id, chart, def, canvas });
  }

  chartRegistry.set(report, handles);

  // New layout: metrics (full width) -> charts (full width) -> findings
  card.appendChild(head);
  card.appendChild(metricsBlock);
  card.appendChild(el('div', { class: 'section-title', style: 'margin-top:16px;' }, [el('h2', {}, ['Charts'])]));
  card.appendChild(chartsBlock);
  card.appendChild(findingsBlock);

  // Optional extras for trailer_history: top event strings (safe; no phone/cell fields exist after scrub)
  if (result.extras?.event_type_top10?.length) {
    const extras = el('div', { style: 'margin-top:12px;' }, [
      el('div', { class: 'section-title' }, [el('h2', {}, ['Top event strings'])]),
      el('ul', { class: 'list' }, result.extras.event_type_top10.map(x =>
        el('li', {}, [`${x.key} — ${x.value}`])
      ))
    ]);
    card.appendChild(extras);
  }

  return card;
}

function renderMetricsGrid(metrics) {
  const grid = el('div', { class: 'kpi-grid' });

  const entries = Object.entries(metrics || {});
  if (!entries.length) {
    grid.appendChild(el('div', { class: 'muted' }, ['No metrics available.']));
    return grid;
  }

  for (const [k, v] of entries) {
    // Skip array values (like top_carriers_for_lost) - these are displayed elsewhere
    if (Array.isArray(v)) continue;

    const label = k.replaceAll('_', ' ');
    const value = (v === null || v === undefined)
      ? '—'
      : (typeof v === 'number' ? formatNumber(v) : String(v));

    grid.appendChild(el('div', { class: 'kpi' }, [
      el('div', { class: 'label' }, [label]),
      el('div', { class: 'value' }, [value]),
      el('div', { class: 'sub muted' }, [''])
    ]));
  }

  return grid;
}

function renderFindings(findings, recs, roi, meta, detentionSpend = null) {
  const wrap = el('div', { style: 'margin-top:10px;' });

  const fTitle = el('div', { class: 'section-title' }, [
    el('h2', {}, ['Findings']),
    el('span', { class: 'muted small' }, [])
  ]);
  wrap.appendChild(fTitle);

  if (!findings.length) {
    wrap.appendChild(el('div', { class: 'muted' }, ['None.']));
  } else {
    const ul = el('ul', { class: 'list' });
    for (const f of findings) {
      // Build the confidence element - with tooltip if reason is available
      const confidenceText = `(${f.confidence || 'medium'} confidence)`;
      const confidenceEl = f.confidenceReason
        ? el('span', {
            class: 'muted small confidence-tooltip',
            'data-tooltip': f.confidenceReason,
            style: 'cursor: help; border-bottom: 1px dotted var(--muted);'
          }, [confidenceText])
        : el('span', { class: 'muted small' }, [confidenceText]);

      // Map level to semantic label with tooltip
      const badgeMeta = {
        green: { label: 'GOOD', tooltip: 'Illustrates improvement in a key area.' },
        yellow: { label: 'CAUTION', tooltip: 'Potential issue related to system use or configuration. PM should investigate.' },
        red: { label: 'BAD', tooltip: 'Illustrates an issue or negative trend in a key area.' }
      }[f.level] || { label: f.level.toUpperCase(), tooltip: '' };

      // Build finding text with glossary term tooltips
      const findingTextSpan = el('span', { class: 'finding-text' });
      for (const node of wrapGlossaryTerms(f.text)) {
        findingTextSpan.appendChild(node);
      }
      findingTextSpan.appendChild(document.createTextNode(' '));
      findingTextSpan.appendChild(confidenceEl);

      ul.appendChild(el('li', { class: 'finding-item' }, [
        el('span', {
          class: `badge ${f.level}`,
          'data-tooltip': badgeMeta.tooltip,
          style: badgeMeta.tooltip ? 'cursor: help;' : ''
        }, [badgeMeta.label]),
        findingTextSpan,
      ]));
    }
    wrap.appendChild(ul);
  }

  const rTitle = el('div', { class: 'section-title', style: 'margin-top:12px;' }, [
    el('h2', {}, ['Recommendations']),
    el('span', { class: 'muted small' }, [])
  ]);
  wrap.appendChild(rTitle);

  if (!recs.length) {
    wrap.appendChild(el('div', { class: 'muted' }, ['None.']));
  } else {
    const ul = el('ul', { class: 'list' });
    for (const r of recs) {
      const li = el('li', {});
      for (const node of wrapGlossaryTerms(r)) {
        li.appendChild(node);
      }
      ul.appendChild(li);
    }
    wrap.appendChild(ul);
  }

  // ROI section
  const roiTitle = el('div', { class: 'section-title', style: 'margin-top:12px;' }, [
    el('h2', {}, ['ROI (estimates)']),
    el('span', { class: 'muted small' }, ['Shown only when assumptions are filled.'])
  ]);
  wrap.appendChild(roiTitle);

  if (!roi && !detentionSpend) {
    wrap.appendChild(el('div', { class: 'muted' }, ['ROI estimates not enabled (missing assumptions).']));
  } else if (roi) {
    const box = el('div', { class: 'callout callout-info' });
    const est = roi.estimate;
    box.appendChild(el('div', { style: 'font-weight:900; color:var(--accent); margin-bottom:6px;' }, [roi.label]));

    if (est) {
      // Display insights as a readable list if available
      if (roi.insights && roi.insights.length > 0) {
        const insightsList = el('ul', { class: 'list', style: 'margin: 8px 0;' });
        for (const insight of roi.insights) {
          insightsList.appendChild(el('li', { style: 'margin-bottom: 4px;' }, [insight]));
        }
        box.appendChild(insightsList);
      }

      // Display key metrics in a more readable format
      const metricsGrid = el('div', { style: 'display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; margin-top: 8px;' });

      // Helper to check for valid numeric values (not null, undefined, or NaN)
      const isValidNum = v => v != null && Number.isFinite(v);

      // Driver performance ROI metrics
      if (isValidNum(est.performance_vs_target_pct)) {
        const color = est.performance_vs_target_pct >= 100 ? 'var(--green)' : 'var(--yellow)';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Performance vs Target']),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${color};` }, [`${est.performance_vs_target_pct}%`])
        ]));
      }

      if (isValidNum(est.avg_moves_per_driver_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Avg Moves/Driver/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.avg_moves_per_driver_per_day)])
        ]));
      }

      if (isValidNum(est.target_moves_per_driver_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Target Moves/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.target_moves_per_driver_per_day)])
        ]));
      }

      if (isValidNum(est.gap_moves_per_day) && est.gap_moves_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Gap to Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--yellow);' }, [`-${est.gap_moves_per_day} moves/day`])
        ]));
      }

      if (isValidNum(est.surplus_moves_per_day) && est.surplus_moves_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Above Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--green);' }, [`+${est.surplus_moves_per_day} moves/day`])
        ]));
      }

      if (isValidNum(est.driver_days_equivalent)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Driver-Days (at target)']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.driver_days_equivalent)])
        ]));
      }

      if (isValidNum(est.money_impact_per_driver_day) && est.money_impact_per_driver_day !== 0) {
        const moneyColor = est.money_impact_per_driver_day > 0 ? 'var(--green)' : 'var(--yellow)';
        const moneySign = est.money_impact_per_driver_day > 0 ? '+' : '-';
        const moneyLabel = est.money_impact_per_driver_day > 0 ? 'Efficiency Gain/Driver/Day' : 'Capacity Gap/Driver/Day';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, [moneyLabel]),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${moneyColor};` }, [`${moneySign}$${Math.abs(est.money_impact_per_driver_day).toFixed(2)}`])
        ]));
      }

      // Trailer History error rate ROI metrics
      if (isValidNum(est.total_errors)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Total Errors']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.total_errors)])
        ]));
      }

      if (isValidNum(est.error_rate_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Errors/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.error_rate_per_day)])
        ]));
      }

      if (isValidNum(est.error_rate_trend_pct) && est.error_rate_trend_direction) {
        const trendColor = est.error_rate_trend_pct <= 0 ? 'var(--green)' : 'var(--yellow)';
        const trendSign = est.error_rate_trend_pct > 0 ? '+' : '';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Error Trend']),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${trendColor};` }, [`${trendSign}${est.error_rate_trend_pct}% (${est.error_rate_trend_direction})`])
        ]));
      }

      // Dock Door ROI metrics
      if (isValidNum(est.avg_turns_per_door_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Avg Turns/Door/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.avg_turns_per_door_per_day)])
        ]));
      }

      if (isValidNum(est.target_turns_per_door_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Target Turns/Door/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.target_turns_per_door_per_day)])
        ]));
      }

      if (isValidNum(est.throughput_vs_target_pct)) {
        const color = est.throughput_vs_target_pct >= 100 ? 'var(--green)' : 'var(--yellow)';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Throughput vs Target']),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${color};` }, [`${est.throughput_vs_target_pct}%`])
        ]));
      }

      if (isValidNum(est.gap_turns_per_day) && est.gap_turns_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Gap to Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--yellow);' }, [`-${est.gap_turns_per_day} turns/door/day`])
        ]));
      }

      if (isValidNum(est.surplus_turns_per_day) && est.surplus_turns_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Above Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--green);' }, [`+${est.surplus_turns_per_day} turns/door/day`])
        ]));
      }

      if (isValidNum(est.idle_door_hours_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Est. Idle Door-Hours/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.idle_door_hours_per_day)])
        ]));
      }

      if (isValidNum(est.cost_of_idle_per_day)) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Est. Idle Cost/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--yellow);' }, [`$${est.cost_of_idle_per_day}`])
        ]));
      }

      // Add staffing analysis metrics if available
      const staffing = roi.staffingAnalysis;
      if (staffing) {
        if (staffing.avgDriversPerDay && staffing.driversNeededAtTarget) {
          const delta = staffing.staffingDelta;
          const deltaColor = Math.abs(delta) <= staffing.driversNeededAtTarget * 0.2 ? 'var(--green)' : 'var(--yellow)';
          const deltaSign = delta > 0 ? '+' : '';
          metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
            el('div', { class: 'muted small' }, ['Avg Drivers/Day vs Needed']),
            el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${deltaColor};` }, [`${staffing.avgDriversPerDay} vs ${staffing.driversNeededAtTarget} (${deltaSign}${delta})`])
          ]));
        }

        if (staffing.topDriverAvgPerDay && staffing.topDriverName) {
          const topVsTarget = staffing.topDriverAvgPerDay >= est.target_moves_per_driver_per_day ? 'var(--green)' : 'inherit';
          metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
            el('div', { class: 'muted small' }, ['Top Performer Avg/Day']),
            el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${topVsTarget};` }, [`${staffing.topDriverAvgPerDay} moves`])
          ]));
        }

        if (staffing.driversNeededIfAllLikeTop) {
          metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
            el('div', { class: 'muted small' }, ['If All Like Top Performer']),
            el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [`Need ~${staffing.driversNeededIfAllLikeTop}/day`])
          ]));
        }
      }

      if (metricsGrid.childNodes.length > 0) {
        box.appendChild(metricsGrid);
      }
    } else {
      box.appendChild(el('div', { class: 'muted' }, ['No estimate available from current data.']));
    }

    box.appendChild(el('div', { class: 'muted small', style: 'margin-top:8px;' }, [roi.disclaimer]));
    wrap.appendChild(box);

    // Render error breakdown table if present (Trailer History)
    if (roi.errorBreakdown && roi.errorBreakdown.length > 0) {
      const table = el('table', { style: 'width: 100%; margin-top: 12px; border-collapse: collapse; font-size: 0.9em;' });
      const thead = el('thead');
      const headerRow = el('tr', { style: 'background: var(--bg-secondary);' });
      headerRow.appendChild(el('th', { style: 'padding: 8px; text-align: left; border-bottom: 1px solid var(--border);' }, ['Error Type']));
      headerRow.appendChild(el('th', { style: 'padding: 8px; text-align: right; border-bottom: 1px solid var(--border);' }, ['Count']));
      headerRow.appendChild(el('th', { style: 'padding: 8px; text-align: right; border-bottom: 1px solid var(--border);' }, ['% of Total']));
      headerRow.appendChild(el('th', { style: 'padding: 8px; text-align: left; border-bottom: 1px solid var(--border);' }, ['Indicates']));
      thead.appendChild(headerRow);
      table.appendChild(thead);

      const tbody = el('tbody');
      for (const err of roi.errorBreakdown) {
        const row = el('tr');
        row.appendChild(el('td', { style: 'padding: 8px; border-bottom: 1px solid var(--border);' }, [err.type]));
        row.appendChild(el('td', { style: 'padding: 8px; text-align: right; border-bottom: 1px solid var(--border); font-weight: 600;' }, [String(err.count)]));
        row.appendChild(el('td', { style: 'padding: 8px; text-align: right; border-bottom: 1px solid var(--border);' }, [`${err.pctOfTotal}%`]));
        row.appendChild(el('td', { style: 'padding: 8px; border-bottom: 1px solid var(--border); color: var(--muted); font-size: 0.9em;' }, [err.indicator]));
        tbody.appendChild(row);
      }
      table.appendChild(tbody);
      box.appendChild(table);
    }
  }

  // Render detention spend analysis if present
  if (detentionSpend) {
    const spendBox = el('div', { class: 'callout callout-info', style: 'margin-top: 12px;' });
    spendBox.appendChild(el('div', { style: 'font-weight:900; color:var(--accent); margin-bottom:6px;' }, [detentionSpend.label]));

    if (detentionSpend.insights && detentionSpend.insights.length > 0) {
      const insightsList = el('ul', { class: 'list', style: 'margin: 8px 0;' });
      for (const insight of detentionSpend.insights) {
        insightsList.appendChild(el('li', { style: 'margin-bottom: 4px;' }, [insight]));
      }
      spendBox.appendChild(insightsList);
    }

    // Only show disclaimer if it has content
    if (detentionSpend.disclaimer) {
      spendBox.appendChild(el('div', { class: 'muted small', style: 'margin-top:8px;' }, [detentionSpend.disclaimer]));
    }
    wrap.appendChild(spendBox);
  }

  return wrap;
}

function formatNumber(n) {
  if (!Number.isFinite(n)) return String(n);
  // If it looks like a percent, keep one decimal (heuristic)
  if (n >= 0 && n <= 100 && Math.round(n * 10) !== Math.round(n) * 10) return `${n.toFixed(1)}`;
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(n);
}

// ---------- CSV helpers for chart/report (kept local for simplicity) ----------
function csvEscape(v) {
  const s = (v === null || v === undefined) ? '' : String(v);
  if (/[,"\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

function buildChartCsvText(def, meta, { timezone, dateRange, report }) {
  // aggregated chart series only
  const cols = def.csv?.columns || [];
  const rows = def.csv?.rows || [];

  const lines = [];
  lines.push(`# report=${report}`);
  lines.push(`# chart=${def.id}`);
  lines.push(`# timezone=${timezone}`);
  lines.push(`# date_range=${dateRange.startDate}..${dateRange.endDate}`);
  lines.push(cols.map(csvEscape).join(','));
  for (const r of rows) {
    lines.push(cols.map(c => csvEscape(r[c])).join(','));
  }
  return lines.join('\n');
}

function buildReportSummaryCsvText(report, result, { timezone, dateRange }) {
  // basic metrics + data quality + findings summary
  const lines = [];
  lines.push(`# report=${report}`);
  lines.push(`# timezone=${timezone}`);
  lines.push(`# date_range=${dateRange.startDate}..${dateRange.endDate}`);

  const cols = ['metric', 'value'];
  lines.push(cols.join(','));

  const metrics = result.metrics || {};
  for (const [k, v] of Object.entries(metrics)) {
    lines.push([csvEscape(k), csvEscape(v)].join(','));
  }

  lines.push([csvEscape('data_quality_score'), csvEscape(result.dataQuality?.score ?? '')].join(','));
  lines.push([csvEscape('data_quality_confidence'), csvEscape(result.dataQuality?.label ?? '')].join(','));

  // findings text (flatten)
  const f = (result.findings || []).map(x => `${x.level.toUpperCase()}: ${x.text} (${x.confidence})`).join(' | ');
  if (f) lines.push([csvEscape('findings'), csvEscape(f)].join(','));

  return lines.join('\n');
}
