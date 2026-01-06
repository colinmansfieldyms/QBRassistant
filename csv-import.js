/**
 * CSV Import UI module for YMS QBR Assistant
 * Handles file uploads, drag-drop, report type selection, and CSV processing
 */

import {
  detectReportType,
  normalizeCSVRow,
  validateCSVColumns,
  validateCSVRow,
  parseCSVFile,
  streamCSVFile,
  aggregateYardAgeBuckets,
} from './csv-parser.js';

// ---------- Report Type Labels ----------

export const REPORT_TYPE_LABELS = {
  current_inventory: 'Current Inventory',
  detention_history: 'Detention History',
  dockdoor_history: 'Dock Door History',
  driver_history: 'Driver History',
  trailer_history: 'Trailer History',
};

// ---------- CSV Import State Manager ----------

/**
 * Creates a CSV import state manager.
 * Tracks uploaded files, their report types, and processing status.
 */
export function createCSVImportState() {
  const files = new Map(); // fileId -> { file, reportType, status, rowCount, columns, error }
  let nextId = 1;

  return {
    /**
     * Adds a file to the import state.
     * @param {File} file - The file object
     * @param {string|null} reportType - Auto-detected or null
     * @returns {string} fileId
     */
    addFile(file, reportType = null) {
      const id = `csv_${nextId++}`;
      files.set(id, {
        id,
        file,
        name: file.name,
        size: file.size,
        reportType,
        status: 'pending', // pending | detecting | ready | processing | done | error
        rowCount: null,
        columns: [],
        error: null,
        validationWarnings: [],
      });
      return id;
    },

    /**
     * Updates file state.
     */
    updateFile(fileId, updates) {
      const current = files.get(fileId);
      if (current) {
        files.set(fileId, { ...current, ...updates });
      }
    },

    /**
     * Removes a file.
     */
    removeFile(fileId) {
      files.delete(fileId);
    },

    /**
     * Gets a file by ID.
     */
    getFile(fileId) {
      return files.get(fileId);
    },

    /**
     * Gets all files.
     */
    getAllFiles() {
      return Array.from(files.values());
    },

    /**
     * Checks if all files have report types assigned.
     */
    allFilesReady() {
      for (const f of files.values()) {
        if (!f.reportType || f.status === 'error') return false;
      }
      return files.size > 0;
    },

    /**
     * Gets files grouped by report type.
     */
    getFilesByReportType() {
      const grouped = {};
      for (const f of files.values()) {
        if (f.reportType) {
          if (!grouped[f.reportType]) grouped[f.reportType] = [];
          grouped[f.reportType].push(f);
        }
      }
      return grouped;
    },

    /**
     * Clears all files.
     */
    clear() {
      files.clear();
      nextId = 1;
    },

    /**
     * Gets the count of files.
     */
    get count() {
      return files.size;
    },
  };
}

// ---------- File Upload Handler ----------

/**
 * Handles file selection/drop and initiates report type detection.
 *
 * @param {FileList|File[]} fileList - Files to process
 * @param {object} csvState - CSV import state manager
 * @param {function} onUpdate - Callback when state changes
 * @returns {Promise<void>}
 */
export async function handleFileUpload(fileList, csvState, onUpdate) {
  const files = Array.from(fileList).filter(f =>
    f.name.toLowerCase().endsWith('.csv') ||
    f.type === 'text/csv' ||
    f.type === 'application/csv'
  );

  if (files.length === 0) {
    return { error: 'No valid CSV files selected' };
  }

  for (const file of files) {
    const fileId = csvState.addFile(file);
    csvState.updateFile(fileId, { status: 'detecting' });
    onUpdate?.();

    try {
      // Parse first few rows to detect report type
      const preview = await parseCSVFile(file);
      const columns = preview.columns;
      const detectedType = detectReportType(columns);

      // Validate columns if type detected
      let validation = { isValid: true, warnings: [] };
      if (detectedType) {
        validation = validateCSVColumns(columns, detectedType);
      }

      csvState.updateFile(fileId, {
        status: detectedType ? 'ready' : 'pending',
        reportType: detectedType,
        columns,
        rowCount: preview.data.length,
        validationWarnings: validation.warnings,
      });
    } catch (err) {
      csvState.updateFile(fileId, {
        status: 'error',
        error: err.message || 'Failed to read file',
      });
    }

    onUpdate?.();
  }

  return { success: true, count: files.length };
}

// ---------- CSV Processing Pipeline ----------

/**
 * Processes all uploaded CSV files and feeds data to analyzers.
 *
 * @param {object} csvState - CSV import state manager
 * @param {object} analyzers - Report analyzers from analysis.js
 * @param {object} options - Processing options
 * @returns {Promise<object>} Processing results
 */
export async function processCSVFiles(csvState, analyzers, options = {}) {
  const {
    timezone = 'America/Los_Angeles',
    onProgress,
    onWarning,
    signal, // AbortSignal for cancellation
  } = options;

  const filesByReport = csvState.getFilesByReportType();
  const results = {
    totalRows: 0,
    rowsByReport: {},
    warnings: [],
    errors: [],
  };

  // Process each report type
  for (const [reportType, files] of Object.entries(filesByReport)) {
    const analyzer = analyzers[reportType];
    if (!analyzer) {
      results.warnings.push(`No analyzer for report type: ${reportType}`);
      continue;
    }

    let reportRows = 0;

    for (const fileInfo of files) {
      if (signal?.aborted) break;

      csvState.updateFile(fileInfo.id, { status: 'processing' });
      onProgress?.({ report: reportType, file: fileInfo.name, status: 'processing' });

      try {
        // For large files (>50MB), use streaming
        if (fileInfo.size > 50 * 1024 * 1024) {
          await processLargeCSV(fileInfo, analyzer, reportType, timezone, {
            onProgress: (processed, total) => {
              onProgress?.({
                report: reportType,
                file: fileInfo.name,
                rowsProcessed: processed,
                totalRows: total,
              });
            },
            onWarning: (msg) => {
              results.warnings.push(`[${fileInfo.name}] ${msg}`);
              onWarning?.(`[${fileInfo.name}] ${msg}`);
            },
            signal,
          });
        } else {
          // Small/medium files: batch processing
          const { data, columns } = await parseCSVFile(fileInfo.file);

          for (let i = 0; i < data.length; i++) {
            if (signal?.aborted) break;

            const normalizedRow = normalizeCSVRow(data[i], reportType, timezone);

            // Validate row
            const rowWarnings = validateCSVRow(normalizedRow, reportType, i);
            rowWarnings.forEach(w => {
              if (results.warnings.length < 100) { // Limit warnings
                results.warnings.push(`[${fileInfo.name}] ${w}`);
              }
            });

            // Feed to analyzer
            analyzer.ingest({
              row: normalizedRow,
              flags: {
                driverContactPresent: !!normalizedRow.driver_cell,
                anyPhoneFieldPresent: !!normalizedRow.driver_cell,
                hasTimezoneArrivalTime: !!normalizedRow.timezone_arrival_time,
                isCSVSource: true, // Flag for CSV-specific handling
              },
              report: reportType,
              timezone,
            });

            reportRows++;
            results.totalRows++;

            // Progress update every 500 rows
            if (i > 0 && i % 500 === 0) {
              onProgress?.({
                report: reportType,
                file: fileInfo.name,
                rowsProcessed: i + 1,
                totalRows: data.length,
              });
            }
          }
        }

        csvState.updateFile(fileInfo.id, { status: 'done', rowCount: reportRows });
        onProgress?.({
          report: reportType,
          file: fileInfo.name,
          status: 'done',
          rowsProcessed: reportRows,
        });
      } catch (err) {
        csvState.updateFile(fileInfo.id, {
          status: 'error',
          error: err.message,
        });
        results.errors.push(`[${fileInfo.name}] ${err.message}`);
        onWarning?.(`Error processing ${fileInfo.name}: ${err.message}`);
      }
    }

    results.rowsByReport[reportType] = reportRows;
  }

  return results;
}

/**
 * Processes large CSV files using streaming.
 */
async function processLargeCSV(fileInfo, analyzer, reportType, timezone, callbacks) {
  const { onProgress, onWarning, signal } = callbacks;

  return new Promise((resolve, reject) => {
    let aborted = false;

    if (signal) {
      signal.addEventListener('abort', () => {
        aborted = true;
      });
    }

    streamCSVFile(
      fileInfo.file,
      reportType,
      timezone,
      {
        onChunk: (rows, totalProcessed) => {
          if (aborted) return;

          for (const row of rows) {
            analyzer.ingest({
              row,
              flags: {
                driverContactPresent: !!row.driver_cell,
                anyPhoneFieldPresent: !!row.driver_cell,
                hasTimezoneArrivalTime: !!row.timezone_arrival_time,
                isCSVSource: true,
              },
              report: reportType,
              timezone,
            });
          }
        },
        onProgress: (processed, total) => {
          if (!aborted) onProgress?.(processed, total);
        },
        onComplete: ({ totalRows }) => {
          resolve({ totalRows });
        },
        onError: (err) => {
          reject(err);
        },
      },
      500 // chunk size
    );
  });
}

// ---------- UI Rendering Helpers ----------

/**
 * Renders the file list HTML.
 * @param {object} csvState - CSV import state manager
 * @returns {string} HTML string
 */
export function renderFileList(csvState) {
  const files = csvState.getAllFiles();

  if (files.length === 0) {
    return '';
  }

  const rows = files.map(f => {
    const statusClass = {
      pending: 'csv-status-pending',
      detecting: 'csv-status-detecting',
      ready: 'csv-status-ready',
      processing: 'csv-status-processing',
      done: 'csv-status-done',
      error: 'csv-status-error',
    }[f.status] || '';

    const statusText = {
      pending: 'Select type',
      detecting: 'Detecting...',
      ready: 'Ready',
      processing: 'Processing...',
      done: 'Done',
      error: f.error || 'Error',
    }[f.status];

    const reportTypeOptions = Object.entries(REPORT_TYPE_LABELS)
      .map(([value, label]) => {
        const selected = f.reportType === value ? 'selected' : '';
        return `<option value="${value}" ${selected}>${label}</option>`;
      })
      .join('');

    const rowCountText = f.rowCount !== null ? `${f.rowCount.toLocaleString()} rows` : '';
    // Escape HTML entities and quotes for safe attribute value
    const escapeAttr = (s) => s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const warningsHtml = f.validationWarnings.length > 0
      ? `<div class="csv-file-warnings" data-tooltip="${escapeAttr(f.validationWarnings.join('\n'))}">${f.validationWarnings.length} warning(s)</div>`
      : '';

    return `
      <div class="csv-file-row" data-file-id="${f.id}">
        <div class="csv-file-info">
          <div class="csv-file-name" title="${f.name}">${truncateFilename(f.name, 25)}</div>
          <div class="csv-file-meta">
            <span class="csv-file-size">${formatFileSize(f.size)}</span>
            ${rowCountText ? `<span class="csv-file-rows">${rowCountText}</span>` : ''}
          </div>
          ${warningsHtml}
        </div>
        <div class="csv-file-type">
          <select class="csv-type-select" data-file-id="${f.id}" ${f.status === 'processing' || f.status === 'done' ? 'disabled' : ''}>
            <option value="">-- Select Type --</option>
            ${reportTypeOptions}
          </select>
        </div>
        <div class="csv-file-status ${statusClass}">${statusText}</div>
        <button class="csv-file-remove btn btn-ghost" data-file-id="${f.id}" title="Remove file" ${f.status === 'processing' ? 'disabled' : ''}>
          &times;
        </button>
      </div>
    `;
  }).join('');

  return `<div class="csv-file-list">${rows}</div>`;
}

/**
 * Renders CSV mode progress display.
 * @param {object} progress - Progress state
 * @returns {string} HTML string
 */
export function renderCSVProgress(progress) {
  const { report, file, rowsProcessed, totalRows, status } = progress;

  if (status === 'done') {
    return `
      <div class="csv-progress-item csv-progress-done">
        <span class="csv-progress-report">${REPORT_TYPE_LABELS[report] || report}</span>
        <span class="csv-progress-file">${file}</span>
        <span class="csv-progress-count">${rowsProcessed?.toLocaleString() || 0} rows</span>
        <span class="csv-progress-status">Done</span>
      </div>
    `;
  }

  const pct = totalRows > 0 ? Math.round((rowsProcessed / totalRows) * 100) : 0;

  return `
    <div class="csv-progress-item">
      <span class="csv-progress-report">${REPORT_TYPE_LABELS[report] || report}</span>
      <span class="csv-progress-file">${file}</span>
      <span class="csv-progress-count">${rowsProcessed?.toLocaleString() || 0} / ${totalRows?.toLocaleString() || '?'} rows (${pct}%)</span>
      <div class="csv-progress-bar">
        <span style="width: ${pct}%"></span>
      </div>
    </div>
  `;
}

// ---------- Utility Functions ----------

function truncateFilename(name, maxLength) {
  if (name.length <= maxLength) return name;
  const ext = name.slice(name.lastIndexOf('.'));
  const base = name.slice(0, name.lastIndexOf('.'));
  const available = maxLength - ext.length - 3;
  return `${base.slice(0, available)}...${ext}`;
}

function formatFileSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// ---------- CSV Mode Disclaimers ----------

export const CSV_MODE_DISCLAIMERS = [
  'CSV import mode has some data limitations compared to API access:',
  'Detention "prevented" counts may be unavailable (pre-detention timestamps not in export)',
  'Date/time values are interpreted in the selected timezone',
];

/**
 * Generates disclaimer HTML for CSV mode results.
 */
export function renderCSVDisclaimers() {
  return `
    <div class="csv-disclaimers callout callout-info">
      <strong>CSV Import Mode</strong>
      <ul>
        ${CSV_MODE_DISCLAIMERS.map(d => `<li>${d}</li>`).join('')}
      </ul>
    </div>
  `;
}

// ---------- Drag and Drop Setup ----------

/**
 * Sets up drag-and-drop event handlers for a drop zone element.
 *
 * @param {HTMLElement} dropZone - The drop zone element
 * @param {function} onDrop - Callback with FileList
 */
export function setupDropZone(dropZone, onDrop) {
  if (!dropZone) return;

  const highlight = () => dropZone.classList.add('csv-drop-active');
  const unhighlight = () => dropZone.classList.remove('csv-drop-active');

  dropZone.addEventListener('dragenter', (e) => {
    e.preventDefault();
    e.stopPropagation();
    highlight();
  });

  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.stopPropagation();
    highlight();
  });

  dropZone.addEventListener('dragleave', (e) => {
    e.preventDefault();
    e.stopPropagation();
    unhighlight();
  });

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    e.stopPropagation();
    unhighlight();

    const files = e.dataTransfer?.files;
    if (files && files.length > 0) {
      onDrop(files);
    }
  });
}
