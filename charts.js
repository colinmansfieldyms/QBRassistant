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
  'period-over-period': 'Compares the average performance of the first half of the date range to the second half, showing if metrics improved or declined over the full period.',
  'overall trend': 'Statistical analysis (linear regression) showing the general direction (increasing, decreasing, or stable) and consistency of a metric across the entire time period.',
  'linear regression': 'A statistical method that finds the best-fit straight line through data points to identify overall trends.',
  'R²': 'R-squared (coefficient of determination) - a statistical measure from 0 to 1 indicating how well the trend line fits the data. Higher values (0.7+) mean a consistent trend; lower values indicate more volatility.',
  'r-squared': 'R-squared (coefficient of determination) - a statistical measure from 0 to 1 indicating how well the trend line fits the data. Higher values (0.7+) mean a consistent trend; lower values indicate more volatility.',
  'volatility': 'Measures how much a metric fluctuates around its trend line. High volatility means unpredictable swings; low volatility means steady, consistent performance.',
  'first half vs second half': 'Splits the date range in two and compares average performance, revealing whether metrics improved or worsened during the period.',
  'consistent trend': 'A stable, predictable pattern with R² ≥ 0.7, indicating the metric follows a clear direction with minimal random variation.',
  'high volatility': 'Unstable performance with R² < 0.4, indicating the metric fluctuates unpredictably with no clear trend.',
};

/**
 * Metrics for facility comparison section.
 * Each metric has thresholds for traffic light scoring and a tooltip explanation.
 */
const COMPARISON_METRICS = {
  // Productivity metrics
  'moves_per_driver_day': {
    label: 'Moves/Driver/Day',
    tooltip: 'Average number of trailer moves completed per driver per day. Higher values indicate better driver productivity and efficient dispatch.',
    category: 'Productivity',
    thresholds: { green: 50, yellow: 35 },
    lowerIsBetter: false,
    unit: '',
    source: 'driver_history',
  },
  'turns_per_door_day': {
    label: 'Turns/Door/Day',
    tooltip: 'Average number of trailers processed through each dock door per day. Higher turns indicate better door utilization and throughput.',
    category: 'Productivity',
    thresholds: { green: 8, yellow: 5 },
    lowerIsBetter: false,
    unit: '',
    source: 'dockdoor_history',
  },

  // Efficiency metrics
  'median_dwell_time': {
    label: 'Median Dwell',
    tooltip: 'Median time trailers spend at dock doors from arrival to departure. Lower dwell times mean faster turnaround and better dock efficiency.',
    category: 'Efficiency',
    thresholds: { green: 60, yellow: 120 },
    lowerIsBetter: true,
    unit: ' min',
    source: 'dockdoor_history',
  },
  'median_queue_time': {
    label: 'Median Queue Time',
    tooltip: 'Median time move requests wait before a driver accepts. Lower queue times indicate responsive drivers and efficient dispatch assignment.',
    category: 'Efficiency',
    thresholds: { green: 5, yellow: 15 },
    lowerIsBetter: true,
    unit: ' min',
    source: 'driver_history',
  },
  'deadhead_ratio': {
    label: 'Deadhead %',
    tooltip: 'Percentage of driver time spent traveling to trailers vs moving them. Lower ratios mean drivers are assigned trailers closer to their location.',
    category: 'Efficiency',
    thresholds: { green: 30, yellow: 50 },
    lowerIsBetter: true,
    unit: '%',
    source: 'driver_history',
  },

  // Quality metrics
  'error_rate': {
    label: 'Error Rate',
    tooltip: 'Percentage of trailers with data errors (lost, yard check inserts, spot edits). Lower rates indicate better gate and yard driver accuracy.',
    category: 'Quality',
    thresholds: { green: 2, yellow: 5 },
    lowerIsBetter: true,
    unit: '%',
    source: 'trailer_history',
  },
  'compliance_rate': {
    label: 'Compliance %',
    tooltip: 'Percentage of moves where drivers properly tap Accept, Start, and Complete in sequence. Higher compliance means better YMS workflow adoption.',
    category: 'Quality',
    thresholds: { green: 90, yellow: 70 },
    lowerIsBetter: false,
    unit: '%',
    source: 'driver_history',
  },
  'detention_rate': {
    label: 'Detentions/Day',
    tooltip: 'Average number of detention events per day. Lower rates indicate better appointment scheduling and dock door management.',
    category: 'Quality',
    thresholds: { green: 5, yellow: 15 },
    lowerIsBetter: true,
    unit: '',
    source: 'detention_history',
  },

  // Utilization metrics
  'process_adoption': {
    label: 'Process Adoption',
    tooltip: 'Percentage of dock door visits where the YMS process feature was used. Higher adoption indicates better utilization of YMS capabilities.',
    category: 'Utilization',
    thresholds: { green: 80, yellow: 50 },
    lowerIsBetter: false,
    unit: '%',
    source: 'dockdoor_history',
  },
  'prevention_rate': {
    label: 'Prevention Rate',
    tooltip: 'Percentage of potential detentions that were prevented through proactive action. Higher rates show effective detention management.',
    category: 'Utilization',
    thresholds: { green: 70, yellow: 40 },
    lowerIsBetter: false,
    unit: '%',
    source: 'detention_history',
  },
};

/**
 * Get traffic light color based on metric value and definition.
 * @param {number} value - The metric value
 * @param {Object} metricDef - The metric definition with thresholds
 * @returns {'green' | 'yellow' | 'red'}
 */
function getTrafficLight(value, metricDef) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 'yellow'; // Unknown/missing data
  }

  const { thresholds, lowerIsBetter } = metricDef;

  if (lowerIsBetter) {
    if (value <= thresholds.green) return 'green';
    if (value <= thresholds.yellow) return 'yellow';
    return 'red';
  } else {
    if (value >= thresholds.green) return 'green';
    if (value >= thresholds.yellow) return 'yellow';
    return 'red';
  }
}

/**
 * Normalize a metric value to 0-100 scale for radar chart.
 * Green zone = 100, Yellow zone = 50-99, Red zone = 0-49
 */
function normalizeMetricValue(value, metricDef) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return 50; // Neutral for unknown
  }

  const { thresholds, lowerIsBetter } = metricDef;

  if (lowerIsBetter) {
    // Lower is better: green threshold is "best", scale up from there
    if (value <= thresholds.green) return 100;
    if (value <= thresholds.yellow) {
      // Scale between 50-99
      const range = thresholds.yellow - thresholds.green;
      const pos = value - thresholds.green;
      return 100 - Math.round((pos / range) * 50);
    }
    // Beyond yellow - scale down to 0
    const badRange = thresholds.yellow * 2; // Assume 2x yellow is really bad
    const pos = value - thresholds.yellow;
    return Math.max(0, 49 - Math.round((pos / badRange) * 49));
  } else {
    // Higher is better: green threshold is "best"
    if (value >= thresholds.green) return 100;
    if (value >= thresholds.yellow) {
      // Scale between 50-99
      const range = thresholds.green - thresholds.yellow;
      const pos = value - thresholds.yellow;
      return 50 + Math.round((pos / range) * 49);
    }
    // Below yellow - scale down to 0
    if (thresholds.yellow > 0) {
      return Math.max(0, Math.round((value / thresholds.yellow) * 49));
    }
    return 0;
  }
}

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

/**
 * Creates a tabbed interface for switching between facilities.
 * @param {Object} options
 * @param {string[]} options.facilities - Array of facility names
 * @param {string} options.activeTab - Currently active tab ('all' or facility name)
 * @param {Object<string, HTMLElement>} options.contentByFacility - Map of facility name to content element
 * @param {Function} options.onTabChange - Callback when tab changes: (facility) => void
 * @returns {HTMLElement} Container element with tabs and content panels
 */
export function createFacilityTabs({ facilities, activeTab = 'all', contentByFacility, onTabChange }) {
  const container = el('div', { class: 'facility-tabs-container' });

  // Dropdown selector instead of horizontal tabs
  const selectorWrap = el('div', { class: 'facility-selector-wrap' });

  const label = el('label', { class: 'facility-selector-label' }, ['View Facility:']);

  const select = el('select', { class: 'facility-selector' });

  // "All Facilities" option
  const allOption = el('option', { value: 'all' }, ['All Facilities']);
  if (activeTab === 'all') allOption.selected = true;
  select.appendChild(allOption);

  // Individual facility options
  for (const fac of facilities) {
    const option = el('option', { value: fac, title: fac }, [fac]);
    if (activeTab === fac) option.selected = true;
    select.appendChild(option);
  }

  selectorWrap.appendChild(label);
  selectorWrap.appendChild(select);
  container.appendChild(selectorWrap);

  // Content panels
  const panelsContainer = el('div', { class: 'facility-tab-panels' });

  // "All Facilities" panel
  const allPanel = el('div', {
    class: `facility-tab-panel${activeTab === 'all' ? ' active' : ''}`,
    'data-tab-panel': 'all',
  });
  if (contentByFacility.all) {
    allPanel.appendChild(contentByFacility.all);
  }
  panelsContainer.appendChild(allPanel);

  // Individual facility panels
  for (const fac of facilities) {
    const panel = el('div', {
      class: `facility-tab-panel${activeTab === fac ? ' active' : ''}`,
      'data-tab-panel': fac,
    });
    if (contentByFacility[fac]) {
      panel.appendChild(contentByFacility[fac]);
    }
    panelsContainer.appendChild(panel);
  }

  container.appendChild(panelsContainer);

  // Dropdown change handler
  select.addEventListener('change', () => {
    const selectedFacility = select.value;

    // Update active panel
    panelsContainer.querySelectorAll('.facility-tab-panel').forEach(p => p.classList.remove('active'));
    const targetPanel = panelsContainer.querySelector(`[data-tab-panel="${selectedFacility}"]`);
    if (targetPanel) targetPanel.classList.add('active');

    // Callback
    onTabChange?.(selectedFacility);
  });

  return container;
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
  // Multi-facility support
  isMultiFacility = false,
  facilities = [],
  getFacilityResult = null, // Function: (facility) => result for that facility
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

  // Track per-facility charts for cleanup
  const perFacilityCharts = new Map(); // facility -> array of chart instances

  // Helper to build content block for a facility (or "all")
  function buildContentBlock(facilityResult, isAllFacilities = false) {
    try {
      const block = el('div', { class: 'facility-content-block' });
      const facility = facilityResult.facility || 'all';

      // Metrics section
      const metricsSection = el('div', {}, [
        el('div', { class: 'section-title' }, [
          el('h2', {}, ['Key metrics']),
          el('button', {
            class: 'btn btn-ghost',
            type: 'button',
            onClick: () => {
              try {
                const csv = buildReportSummaryCsvText(report, facilityResult, { timezone, dateRange });
                downloadText(`report_${report}_summary.csv`, csv);
              } catch (e) {
                onWarning?.(`CSV export failed for ${report}: ${e?.message || String(e)}`);
              }
            }
          }, ['⬇ CSV'])
        ]),
        renderMetricsGrid(facilityResult.metrics || {})
      ]);
      block.appendChild(metricsSection);

      // Charts section - only render charts for "all" view to avoid Chart.js conflicts
      if (isAllFacilities) {
        block.appendChild(el('div', { class: 'section-title', style: 'margin-top:16px;' }, [el('h2', {}, ['Charts'])]));
        block.appendChild(chartsBlock);
      } else if (facilityResult.charts?.length) {
        // For per-facility, create NEW chart instances and track them for cleanup
        const facilityChartsBlock = el('div', { class: 'chart-grid' });
        const chartsForThisFacility = [];

        for (const def of facilityResult.charts) {
          const chartCard = el('div', { class: 'chart-card' });
          const canvas = el('canvas', { width: 800, height: 360 });
          const wrap = el('div', { class: 'canvas-wrap', style: 'height:360px;' }, [canvas]);

          const title = el('div', { class: 'chart-title' }, [el('b', {}, [def.title])]);
          const desc = def.description ? el('div', { class: 'muted small', style: 'margin-bottom:8px;' }, [def.description]) : null;

          chartCard.appendChild(title);
          if (desc) chartCard.appendChild(desc);
          chartCard.appendChild(wrap);
          facilityChartsBlock.appendChild(chartCard);

          // Render chart with error handling
          try {
            const ctx = canvas.getContext('2d');
            if (!ctx) {
              console.error('Failed to get 2d context for per-facility chart');
              continue;
            }
            const cfg = chartConfigFromKind(def.kind, def.data, def.title, partialPeriodMode);
            const chart = new window.Chart(ctx, cfg);

            // Track this chart for cleanup
            chartsForThisFacility.push(chart);
          } catch (error) {
            console.error(`Error creating chart for facility ${facility}:`, error);
          }
        }

        // Store charts for this facility
        if (chartsForThisFacility.length > 0) {
          perFacilityCharts.set(facility, chartsForThisFacility);
        }

        block.appendChild(el('div', { class: 'section-title', style: 'margin-top:16px;' }, [el('h2', {}, ['Charts'])]));
        block.appendChild(facilityChartsBlock);
      }

      // Findings section
      const findingsSection = el('div', { style: 'margin-top:16px;' }, [
        renderFindings(
          facilityResult.findings || [],
          facilityResult.recommendations || [],
          facilityResult.roi || null,
          facilityResult.meta || {},
          facilityResult.detentionSpend || null
        )
      ]);
      block.appendChild(findingsSection);

      // Optional extras for trailer_history
      if (facilityResult.extras?.event_type_top10?.length) {
        const extras = el('div', { style: 'margin-top:12px;' }, [
          el('div', { class: 'section-title' }, [el('h2', {}, ['Top event strings'])]),
          el('ul', { class: 'list' }, facilityResult.extras.event_type_top10.map(x =>
            el('li', {}, [`${x.key} — ${x.value}`])
          ))
        ]);
        block.appendChild(extras);
      }

      return block;

    } catch (error) {
      console.error(`Error building content block:`, error);
      return el('div', { class: 'error-message muted', style: 'padding: 24px;' }, [
        `Error rendering facility data: ${error.message}`
      ]);
    }
  }

  // Append header (always shown outside tabs)
  card.appendChild(head);

  // Multi-facility: wrap content in tabs
  if (isMultiFacility && facilities.length > 0 && getFacilityResult) {
    // Build content for "all" (current aggregated result) - always loaded upfront
    const contentByFacility = {
      all: buildContentBlock(result, true)
    };

    // For small facility counts (<= 5), build all content upfront for better UX
    // For larger counts, use lazy loading to manage memory
    const shouldLazyLoad = facilities.length > 5;

    if (!shouldLazyLoad) {
      // Eager loading: Build all facility content upfront
      for (const fac of facilities) {
        try {
          const facResult = getFacilityResult(fac);
          if (facResult) {
            contentByFacility[fac] = buildContentBlock(facResult, false);
          } else {
            contentByFacility[fac] = el('div', { class: 'muted', style: 'padding: 24px;' }, [
              `No data available for ${fac}`,
              el('div', { class: 'muted small', style: 'margin-top: 8px;' }, [
                'This facility may not have sufficient data in the selected date range.'
              ])
            ]);
          }
        } catch (error) {
          console.error(`Error building content for facility ${fac}:`, error);
          contentByFacility[fac] = el('div', { class: 'error-message muted', style: 'padding: 24px;' }, [
            `Error loading data for ${fac}: ${error.message}`
          ]);
        }
      }
    }

    const tabs = createFacilityTabs({
      facilities,
      activeTab: 'all',
      contentByFacility,
      onTabChange: (newFacility) => {
        // Lazy loading: Build content on-demand when tab is clicked
        if (shouldLazyLoad && newFacility !== 'all' && !contentByFacility[newFacility]) {
          try {
            const facResult = getFacilityResult(newFacility);
            if (facResult) {
              const content = buildContentBlock(facResult, false);
              contentByFacility[newFacility] = content;

              // Find the panel for this facility and append content
              const panel = card.querySelector(`[data-tab-panel="${newFacility}"]`);
              if (panel) {
                panel.innerHTML = '';
                panel.appendChild(content);
              }
            } else {
              const content = el('div', { class: 'muted', style: 'padding: 24px;' }, [
                `No data available for ${newFacility}`
              ]);
              contentByFacility[newFacility] = content;
              const panel = card.querySelector(`[data-tab-panel="${newFacility}"]`);
              if (panel) {
                panel.innerHTML = '';
                panel.appendChild(content);
              }
            }
          } catch (error) {
            console.error(`Error lazy-loading content for facility ${newFacility}:`, error);
          }
        }

        // Memory optimization: Destroy charts from previously active facility
        // (except "all" which is in chartRegistry and managed separately)
        const previousFacility = card._lastActiveFacility;
        if (shouldLazyLoad && previousFacility && previousFacility !== 'all' && previousFacility !== newFacility) {
          const chartsToDestroy = perFacilityCharts.get(previousFacility);
          if (chartsToDestroy) {
            chartsToDestroy.forEach(chart => {
              try {
                chart.destroy();
              } catch (e) {
                console.warn(`Failed to destroy chart:`, e);
              }
            });
            perFacilityCharts.delete(previousFacility);

            // Clear the panel content to fully release memory
            const panel = card.querySelector(`[data-tab-panel="${previousFacility}"]`);
            if (panel) {
              panel.innerHTML = '';
            }
            delete contentByFacility[previousFacility];
          }
        }

        // Track last active facility
        card._lastActiveFacility = newFacility;
      }
    });
    card.appendChild(tabs);
  } else {
    // Single-facility: use existing layout directly
    card.appendChild(metricsBlock);
    card.appendChild(el('div', { class: 'section-title', style: 'margin-top:16px;' }, [el('h2', {}, ['Charts'])]));
    card.appendChild(chartsBlock);
    card.appendChild(findingsBlock);

    // Optional extras for trailer_history: top event strings
    if (result.extras?.event_type_top10?.length) {
      const extras = el('div', { style: 'margin-top:12px;' }, [
        el('div', { class: 'section-title' }, [el('h2', {}, ['Top event strings'])]),
        el('ul', { class: 'list' }, result.extras.event_type_top10.map(x =>
          el('li', {}, [`${x.key} — ${x.value}`])
        ))
      ]);
      card.appendChild(extras);
    }
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

/**
 * Extract comparison metrics from results by facility.
 * Returns a map of facility -> metricKey -> value
 */
function extractFacilityMetrics(results, facilities, getFacilityResult) {
  const metricsByFacility = {};

  // Initialize structure
  for (const fac of facilities) {
    metricsByFacility[fac] = {
      // Productivity
      moves_per_driver_day: null,
      turns_per_door_day: null,

      // Efficiency
      median_dwell_time: null,
      median_queue_time: null,
      deadhead_ratio: null,

      // Quality
      error_rate: null,
      compliance_rate: null,
      detention_rate: null,

      // Utilization
      process_adoption: null,
      prevention_rate: null,
    };
  }

  // Extract metrics from each report's per-facility results
  const reportTypes = Object.keys(results);

  for (const report of reportTypes) {
    for (const fac of facilities) {
      // Call getFacilityResult() to get per-facility data
      const facResult = getFacilityResult(report, fac);
      if (!facResult || !facResult.metrics) continue;

      const metrics = facResult.metrics;

      // Map metric keys from per-facility result to comparison metric keys
      switch (report) {
        case 'driver_history':
          metricsByFacility[fac].moves_per_driver_day =
            metrics.avg_moves_per_driver_per_day ?? metrics.avgMovesPerDriverPerDay ?? null;
          metricsByFacility[fac].median_queue_time =
            metrics.median_queue_time_min ?? metrics.medianQueueTimeMin ?? null;
          metricsByFacility[fac].deadhead_ratio =
            metrics.deadhead_pct ?? metrics.deadheadPct ?? null;
          metricsByFacility[fac].compliance_rate =
            metrics.compliance_rate ?? metrics.complianceRate ?? null;
          break;

        case 'dockdoor_history':
          metricsByFacility[fac].turns_per_door_day =
            metrics.avg_turns_per_door_per_day ?? metrics.avgTurnsPerDoorPerDay ?? null;
          metricsByFacility[fac].median_dwell_time =
            metrics.median_dwell_time_min ?? metrics.medianDwellTimeMin ?? null;
          metricsByFacility[fac].process_adoption =
            metrics.process_adoption_pct ?? metrics.processAdoptionPct ?? null;
          break;

        case 'trailer_history':
          metricsByFacility[fac].error_rate =
            metrics.error_rate_pct ?? metrics.errorRatePct ?? metrics.lost_pct ?? null;
          break;

        case 'detention_history':
          // Calculate detentions per day if not already calculated
          if (metrics.detentions_per_day !== undefined) {
            metricsByFacility[fac].detention_rate = metrics.detentions_per_day;
          } else if (metrics.detention_events !== undefined && facResult.meta) {
            const totalDetentions = metrics.detention_events ?? 0;
            const daysInPeriod = calculateDaysInPeriod(facResult.meta);
            metricsByFacility[fac].detention_rate = daysInPeriod > 0
              ? Math.round((totalDetentions / daysInPeriod) * 10) / 10
              : null;
          }
          metricsByFacility[fac].prevention_rate =
            metrics.prevention_rate_pct ?? metrics.preventionRatePct ?? metrics.prevention_rate ?? null;
          break;
      }
    }
  }

  return metricsByFacility;
}

// Helper to calculate days in period from meta
function calculateDaysInPeriod(meta) {
  if (!meta?.startDate || !meta?.endDate) return 0;
  const start = new Date(meta.startDate);
  const end = new Date(meta.endDate);
  return Math.ceil((end - start) / (1000 * 60 * 60 * 24));
}

/**
 * Render the Facility Comparisons section.
 * Only renders if 2+ facilities are detected.
 */
export function renderFacilityComparisons({ facilities, results, chartRegistry, getFacilityResult }) {
  if (!facilities || facilities.length < 2) {
    return null; // Don't render for single facility
  }

  const section = el('div', { class: 'report-card facility-comparison-section' });

  // Collapsible header
  const header = el('details', { open: true });
  const summary = el('summary', { class: 'section-title', style: 'cursor: pointer;' }, [
    el('h2', {}, ['Facility Comparisons']),
    el('span', { class: 'badge blue' }, [`${facilities.length} facilities`]),
  ]);
  header.appendChild(summary);

  const content = el('div', { class: 'comparison-content', style: 'margin-top: 16px;' });

  // Extract metrics for comparison
  const metricsByFacility = extractFacilityMetrics(results, facilities, getFacilityResult);

  // Grid: radar chart + summary table
  const grid = el('div', { class: 'comparison-grid' });

  // Radar Chart
  const radarCard = el('div', { class: 'chart-card comparison-card radar-card' });

  // Action buttons for radar chart
  const actionButtons = [];

  actionButtons.push(
    el('button', {
      class: 'btn btn-ghost',
      type: 'button',
      title: 'View fullscreen'
    }, ['⛶ Expand']),

    el('button', {
      class: 'btn btn-ghost',
      type: 'button',
      title: 'Download as PNG'
    }, ['⬇ PNG']),

    el('button', {
      class: 'btn btn-ghost',
      type: 'button',
      title: 'Download as CSV'
    }, ['⬇ CSV'])
  );

  const actions = el('div', { class: 'chart-actions' }, actionButtons);

  const titleContent = [
    el('b', {}, ['Performance Radar']),
    el('span', { class: 'muted small', style: 'margin-left: 8px;' }, ['Normalized scores (0-100)']),
    actions
  ];

  const radarTitle = el('div', { class: 'chart-title' }, titleContent);
  const radarCanvas = el('canvas', { width: 400, height: 400 });
  const radarWrap = el('div', { class: 'canvas-wrap', style: 'height: 400px;' }, [radarCanvas]);

  radarCard.appendChild(radarTitle);
  radarCard.appendChild(radarWrap);

  // Build radar chart data - only include metrics from uploaded report types
  const availableReportTypes = new Set(Object.keys(results));
  const radarLabels = [];
  const metricKeys = [];
  for (const [key, def] of Object.entries(COMPARISON_METRICS)) {
    // Only include metrics whose source report type is present in uploaded data
    if (availableReportTypes.has(def.source)) {
      radarLabels.push(def.label);
      metricKeys.push(key);
    }
  }

  // Generate distinct colors for facilities
  const facilityColors = [
    { bg: 'rgba(99, 102, 241, 0.2)', border: 'rgb(99, 102, 241)' },   // Indigo
    { bg: 'rgba(34, 197, 94, 0.2)', border: 'rgb(34, 197, 94)' },     // Green
    { bg: 'rgba(249, 115, 22, 0.2)', border: 'rgb(249, 115, 22)' },   // Orange
    { bg: 'rgba(236, 72, 153, 0.2)', border: 'rgb(236, 72, 153)' },   // Pink
    { bg: 'rgba(14, 165, 233, 0.2)', border: 'rgb(14, 165, 233)' },   // Sky
    { bg: 'rgba(168, 85, 247, 0.2)', border: 'rgb(168, 85, 247)' },   // Purple
  ];

  const radarDatasets = facilities.slice(0, 10).map((fac, idx) => {
    const colorIdx = idx % facilityColors.length;
    const data = metricKeys.map(key => {
      const value = metricsByFacility[fac]?.[key];
      const def = COMPARISON_METRICS[key];
      return normalizeMetricValue(value, def);
    });

    return {
      label: fac,
      data,
      backgroundColor: facilityColors[colorIdx].bg,
      borderColor: facilityColors[colorIdx].border,
      borderWidth: 2,
      pointBackgroundColor: facilityColors[colorIdx].border,
    };
  });

  const radarConfig = {
    type: 'radar',
    data: {
      labels: radarLabels,
      datasets: radarDatasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          beginAtZero: true,
          max: 100,
          ticks: {
            stepSize: 25,
            display: false,
          },
          pointLabels: {
            font: { size: 11 },
          },
        },
      },
      plugins: {
        legend: {
          position: 'bottom',
          labels: { usePointStyle: true, padding: 12 },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.raw}/100`,
          },
        },
      },
    },
  };

  // Create radar chart
  const radarChart = new window.Chart(radarCanvas.getContext('2d'), radarConfig);
  if (chartRegistry) {
    if (!chartRegistry.has('facility_comparison')) {
      chartRegistry.set('facility_comparison', []);
    }
    chartRegistry.get('facility_comparison').push({ id: 'radar', chart: radarChart });
  }

  // Wire up action button handlers
  actionButtons[0].addEventListener('click', () => {
    try {
      openRadarChartFullscreen(radarCanvas, facilities, metricsByFacility, metricKeys);
    } catch (e) {
      console.error('Fullscreen failed:', e);
    }
  });

  actionButtons[1].addEventListener('click', () => {
    try {
      downloadPngFromCanvas(radarCanvas, 'facility_comparison_radar.png');
    } catch (e) {
      console.error('PNG export failed:', e);
    }
  });

  actionButtons[2].addEventListener('click', () => {
    try {
      const csvText = buildRadarChartCsvText(facilities, metricsByFacility, metricKeys);
      downloadText('facility_comparison_scores.csv', csvText);
    } catch (e) {
      console.error('CSV export failed:', e);
    }
  });

  // Summary Table
  const tableCard = el('div', { class: 'chart-card' });
  const tableTitle = el('div', { class: 'chart-title' }, [el('b', {}, ['Metric Summary'])]);
  const tableDesc = el('div', { class: 'muted small', style: 'margin-bottom: 8px;' }, [
    'Traffic light scoring: ',
    el('span', { class: 'traffic-green', style: 'padding: 2px 6px; border-radius: 3px; margin: 0 4px;' }, ['Good']),
    el('span', { class: 'traffic-yellow', style: 'padding: 2px 6px; border-radius: 3px; margin: 0 4px;' }, ['Caution']),
    el('span', { class: 'traffic-red', style: 'padding: 2px 6px; border-radius: 3px; margin: 0 4px;' }, ['Attention']),
  ]);

  const table = el('table', { class: 'comparison-table' });
  const thead = el('thead');
  const headerRow = el('tr');
  headerRow.appendChild(el('th', {}, ['Metric']));
  for (const fac of facilities.slice(0, 10)) {
    headerRow.appendChild(el('th', {}, [fac]));
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = el('tbody');
  // Only iterate over metrics from uploaded report types (using metricKeys which is already filtered)
  for (const key of metricKeys) {
    const def = COMPARISON_METRICS[key];
    const row = el('tr');

    // Metric name with tooltip
    const nameCell = el('td', { class: 'metric-name' });
    const nameTrigger = el('span', {
      class: 'tooltip-trigger',
      'data-tooltip': def.tooltip,
    }, [def.label]);
    nameCell.appendChild(nameTrigger);
    row.appendChild(nameCell);

    // Value cells for each facility
    for (const fac of facilities.slice(0, 10)) {
      const value = metricsByFacility[fac]?.[key];
      const light = getTrafficLight(value, def);
      const displayValue = value !== null && value !== undefined && Number.isFinite(value)
        ? `${formatNumber(value)}${def.unit}`
        : '—';

      const cell = el('td', {
        class: `metric-cell traffic-${light}`,
      }, [displayValue]);
      row.appendChild(cell);
    }

    tbody.appendChild(row);
  }
  table.appendChild(tbody);

  const tableWrap = el('div', { style: 'overflow-x: auto;' }, [table]);
  tableCard.appendChild(tableTitle);
  tableCard.appendChild(tableDesc);
  tableCard.appendChild(tableWrap);

  grid.appendChild(radarCard);
  grid.appendChild(tableCard);

  content.appendChild(grid);
  header.appendChild(content);
  section.appendChild(header);

  return section;
}

/**
 * Open radar chart in fullscreen modal
 */
function openRadarChartFullscreen(canvas, facilities, metricsByFacility, metricKeys) {
  // Create modal backdrop
  const modal = el('div', {
    class: 'chart-modal',
    style: 'position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 9999; display: flex; align-items: center; justify-content: center; padding: 20px;'
  });

  // Create modal content
  const content = el('div', {
    style: 'background: white; border-radius: 8px; padding: 24px; max-width: 90vw; max-height: 90vh; overflow: auto; position: relative;'
  });

  // Close button
  const closeBtn = el('button', {
    type: 'button',
    class: 'btn btn-ghost',
    style: 'position: absolute; top: 16px; right: 16px; z-index: 1;'
  }, ['✕ Close']);

  // Title
  const title = el('h2', { style: 'margin-bottom: 16px;' }, ['Facility Performance Radar - Normalized Scores']);

  // Canvas for fullscreen chart
  const fullscreenCanvas = el('canvas', { width: 800, height: 800 });
  const canvasWrap = el('div', { class: 'canvas-wrap', style: 'height: 800px; width: 800px; margin: 0 auto;' }, [fullscreenCanvas]);

  content.appendChild(closeBtn);
  content.appendChild(title);
  content.appendChild(canvasWrap);
  modal.appendChild(content);

  // Build radar chart data for fullscreen
  const radarLabels = metricKeys.map(key => COMPARISON_METRICS[key].label);

  const facilityColors = [
    { bg: 'rgba(99, 102, 241, 0.2)', border: 'rgb(99, 102, 241)' },
    { bg: 'rgba(34, 197, 94, 0.2)', border: 'rgb(34, 197, 94)' },
    { bg: 'rgba(249, 115, 22, 0.2)', border: 'rgb(249, 115, 22)' },
    { bg: 'rgba(236, 72, 153, 0.2)', border: 'rgb(236, 72, 153)' },
    { bg: 'rgba(14, 165, 233, 0.2)', border: 'rgb(14, 165, 233)' },
    { bg: 'rgba(168, 85, 247, 0.2)', border: 'rgb(168, 85, 247)' },
  ];

  const radarDatasets = facilities.slice(0, 10).map((fac, idx) => {
    const colorIdx = idx % facilityColors.length;
    const data = metricKeys.map(key => {
      const value = metricsByFacility[fac]?.[key];
      const def = COMPARISON_METRICS[key];
      return normalizeMetricValue(value, def);
    });

    return {
      label: fac,
      data,
      backgroundColor: facilityColors[colorIdx].bg,
      borderColor: facilityColors[colorIdx].border,
      borderWidth: 2,
      pointBackgroundColor: facilityColors[colorIdx].border,
    };
  });

  const radarConfig = {
    type: 'radar',
    data: {
      labels: radarLabels,
      datasets: radarDatasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        r: {
          beginAtZero: true,
          max: 100,
          ticks: {
            stepSize: 25,
            font: { size: 12 },
          },
          pointLabels: {
            font: { size: 13 },
          },
        },
      },
      plugins: {
        legend: {
          position: 'bottom',
          labels: { usePointStyle: true, padding: 16, font: { size: 14 } },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.raw}/100`,
          },
        },
      },
    },
  };

  // Create fullscreen chart
  const fullscreenChart = new window.Chart(fullscreenCanvas.getContext('2d'), radarConfig);

  // Close modal handler
  const closeModal = () => {
    fullscreenChart.destroy();
    modal.remove();
  };

  closeBtn.addEventListener('click', closeModal);
  modal.addEventListener('click', (e) => {
    if (e.target === modal) closeModal();
  });

  document.body.appendChild(modal);
}

/**
 * Build CSV text from radar chart data
 */
function buildRadarChartCsvText(facilities, metricsByFacility, metricKeys) {
  const lines = [];

  // Header row
  lines.push(['Metric', ...facilities].join(','));

  // Data rows
  for (const metricKey of metricKeys) {
    const def = COMPARISON_METRICS[metricKey];
    const row = [def.label];

    for (const fac of facilities) {
      const value = metricsByFacility[fac]?.[metricKey];
      const normalized = normalizeMetricValue(value, def);
      row.push(normalized);
    }

    lines.push(row.join(','));
  }

  return lines.join('\n');
}
