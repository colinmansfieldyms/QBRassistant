import { downloadText } from './export.js?v=2025.01.07.0';

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

function chartConfigFromKind(kind, chartData, title) {
  const base = {
    type: kind === 'pie' ? 'pie' : (kind === 'bar' ? 'bar' : 'line'),
    data: chartData,
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom' },
        title: { display: false, text: title }
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

function openChartFullscreen(def, chartData, report, onWarning) {
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
  const cfg = chartConfigFromKind(def.kind, chartData, def.title);

  // Override config for fullscreen to show all labels
  if (cfg.options.scales && cfg.options.scales.x) {
    cfg.options.scales.x.ticks = {
      maxRotation: 45,
      minRotation: 0,
      autoSkip: false, // Show all labels
      font: { size: 11 }
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

export function renderReportResult({
  report,
  result,
  timezone,
  dateRange,
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
      el('h3', {}, [report]),
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
      }, ['Download aggregated report summary CSV'])
    ]),
    renderMetricsGrid(result.metrics || {})
  ]);

  const findingsBlock = el('div', {}, [
    el('div', { class: 'section-title' }, [
      el('h2', {}, ['Findings & recommendations']),
      el('span', { class: 'muted small' }, ['Heuristics-based flags; see confidence labels.'])
    ]),
    renderFindings(result.findings || [], result.recommendations || [], result.roi || null, result.meta || {})
  ]);

  const chartsBlock = el('div', { class: 'chart-grid' });
  const handles = [];

  for (const def of (result.charts || [])) {
    const chartCard = el('div', { class: 'chart-card' });
    const canvas = el('canvas', { width: 800, height: 360 });
    const wrap = el('div', { class: 'canvas-wrap', style: 'height:360px;' }, [canvas]);

    const actions = el('div', { class: 'chart-actions' }, [
      el('button', {
        class: 'btn btn-ghost',
        type: 'button',
        onClick: () => {
          try {
            openChartFullscreen(def, def.data, report, onWarning);
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
      }, ['Download PNG']),
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
      }, ['⬇ CSV']),
    ]);

    const title = el('div', { class: 'chart-title' }, [
      el('b', {}, [def.title]),
      actions
    ]);

    const desc = def.description ? el('div', { class: 'muted small', style: 'margin-bottom:8px;' }, [def.description]) : null;

    chartCard.appendChild(title);
    if (desc) chartCard.appendChild(desc);
    chartCard.appendChild(wrap);

    chartsBlock.appendChild(chartCard);

    // Render chart
    const cfg = chartConfigFromKind(def.kind, def.data, def.title);
    const chart = new window.Chart(canvas.getContext('2d'), cfg);
    handles.push({ id: def.id, chart, def, canvas });
  }

  chartRegistry.set(report, handles);

  const twoCol = el('div', { class: 'two-col' }, [
    metricsBlock,
    el('div', {}, [
      el('div', { class: 'section-title' }, [el('h2', {}, ['Charts'])]),
      chartsBlock
    ])
  ]);

  card.appendChild(head);
  card.appendChild(twoCol);
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

function renderFindings(findings, recs, roi, meta) {
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
      ul.appendChild(el('li', {}, [
        el('span', { class: `badge ${f.level}` }, [f.level.toUpperCase()]),
        document.createTextNode(` ${f.text} `),
        el('span', { class: 'muted small' }, [`(${f.confidence || 'medium'} confidence)`]),
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
    for (const r of recs) ul.appendChild(el('li', {}, [r]));
    wrap.appendChild(ul);
  }

  // ROI section
  const roiTitle = el('div', { class: 'section-title', style: 'margin-top:12px;' }, [
    el('h2', {}, ['ROI (estimates)']),
    el('span', { class: 'muted small' }, ['Shown only when assumptions are filled.'])
  ]);
  wrap.appendChild(roiTitle);

  if (!roi) {
    wrap.appendChild(el('div', { class: 'muted' }, ['ROI estimates not enabled (missing assumptions).']));
  } else {
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

      if (est.performance_vs_target_pct !== null) {
        const color = est.performance_vs_target_pct >= 100 ? 'var(--green)' : 'var(--yellow)';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Performance vs Target']),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${color};` }, [`${est.performance_vs_target_pct}%`])
        ]));
      }

      if (est.avg_moves_per_driver_per_day !== null) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Avg Moves/Driver/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.avg_moves_per_driver_per_day)])
        ]));
      }

      if (est.target_moves_per_driver_per_day !== null) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Target Moves/Day']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.target_moves_per_driver_per_day)])
        ]));
      }

      if (est.gap_moves_per_day !== null && est.gap_moves_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Gap to Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--yellow);' }, [`-${est.gap_moves_per_day} moves/day`])
        ]));
      }

      if (est.surplus_moves_per_day !== null && est.surplus_moves_per_day > 0) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Above Target']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em; color: var(--green);' }, [`+${est.surplus_moves_per_day} moves/day`])
        ]));
      }

      if (est.driver_days_equivalent !== null) {
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, ['Driver-Days (at target)']),
          el('div', { style: 'font-weight: 700; font-size: 1.2em;' }, [String(est.driver_days_equivalent)])
        ]));
      }

      if (est.money_impact_per_driver_day !== null && est.money_impact_per_driver_day !== 0) {
        const moneyColor = est.money_impact_per_driver_day > 0 ? 'var(--green)' : 'var(--yellow)';
        const moneySign = est.money_impact_per_driver_day > 0 ? '+' : '-';
        const moneyLabel = est.money_impact_per_driver_day > 0 ? 'Efficiency Gain/Driver/Day' : 'Capacity Gap/Driver/Day';
        metricsGrid.appendChild(el('div', { style: 'padding: 8px; background: var(--bg-secondary); border-radius: 4px;' }, [
          el('div', { class: 'muted small' }, [moneyLabel]),
          el('div', { style: `font-weight: 700; font-size: 1.2em; color: ${moneyColor};` }, [`${moneySign}$${Math.abs(est.money_impact_per_driver_day).toFixed(2)}`])
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
