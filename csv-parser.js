/**
 * CSV Parser module for YMS QBR Assistant
 * Handles CSV parsing, field mapping, validation, and report type detection
 */

// ---------- Field Mapping Configuration ----------

/**
 * Maps CSV column names to API field names per report type.
 * Keys are CSV column headers, values are the normalized API field names.
 */
export const CSV_FIELD_MAPS = {
  current_inventory: {
    // Existing standard column mappings
    'Appointment Date': 'appointment_date',
    'Appointment Time': 'appointment_time',
    'Arrival Time': 'timezone_arrival_time',
    'Move Type': 'move_type_name',
    'SCAC': 'scac',
    'Trailer Number': 'trailer_number',
    'Load Status': 'load_status',
    'Live Load': 'live_load',
    'Elapsed Time (Hours)': 'csv_elapsed_hours',
    'Latest Loaded Time': 'csv_latest_loaded',
    'Updated At': 'updated_at',
    'Carrier SCAC': 'carrier_scac',
    'Driver Cell': 'driver_cell',
    'Drop Spot': 'drop_spot',
    'Drop Facility': 'drop_facility',
    // Team data dump mappings
    'move_number': 'appointment_number',
    'trailer_number': 'trailer_number',
    'scac': 'scac',
    'reference_type_value_1': 'reference_1',
    'reference_type_value_2': 'reference_2',
    'load_quantity_1': 'load_quantity',
    'origin': 'origin',
    'seal_number': 'seal_number',
    'comments': 'comments',
    'arrival_time': 'timezone_arrival_time',
    'updated_at': 'updated_at',
    'driver_cell_number': 'driver_cell',
    'appointment_date': 'appointment_date',
    'appointment_time': 'appointment_time',
    'trailer_condition_name': 'trailer_condition_name',
    'customer_name': 'customer_name',
    'supplier_name': 'supplier_name',
    'move_type_name': 'move_type_name',
    'load_status_name': 'load_status',
    'load_type_name': 'load_type_name',
    'sub_load_type_name': 'sub_load_type_name',
    'trailer_type_name': 'trailer_type_name',
    'trailer_size_name': 'trailer_size_name',
    'drop_facility_name': 'drop_facility',
    'drop_spot_name': 'drop_spot',
    'move_status_name': 'move_status_name',
    'priority_move': 'priority_move',
    'priority_load_name': 'priority_load_name',
    'elapsed_time': 'csv_elapsed_hours',
    'latest_loaded_time': 'csv_latest_loaded',
    'a_live_load': 'live_load',
    // Additional standard column aliases
    'Appointment Number': 'appointment_number',
    'Reference 1': 'reference_1',
    'Reference 2': 'reference_2',
    'Load Quantity': 'load_quantity',
    'Origin': 'origin',
    'Seal Number': 'seal_number',
    'Comments': 'comments',
    'Driver Cell Number': 'driver_cell',
    'Trailer Condition': 'trailer_condition_name',
    'Customer': 'customer_name',
    'Supplier': 'supplier_name',
    'Load Type': 'load_type_name',
    'Sub Load Type': 'sub_load_type_name',
    'Trailer Type': 'trailer_type_name',
    'Trailer Size': 'trailer_size_name',
    'Move Status': 'move_status_name',
    'Priority Move': 'priority_move',
    'Priority Load': 'priority_load_name',
  },

  detention_history: {
    // Existing standard column mappings
    'Appt Date': 'csv_appt_date',
    'Appt Time': 'csv_appt_time',
    'Arrival Date': 'csv_arrival_date',
    'Arrival Time': 'csv_arrival_time',
    'Pre Detention Date': 'csv_predetention_date',
    'Pre Detention Time': 'csv_predetention_time',
    'Detention Date': 'csv_detention_date',
    'Detention Time': 'csv_detention_time',
    'Departure Date': 'csv_departure_date',
    'Departure Time': 'csv_departure_time',
    'Process Complete Date': 'csv_process_complete_date',
    'Process Complete Time': 'csv_process_complete_time',
    'Early/Late': 'csv_early_late',
    'Early/Late (Minutes)': 'csv_early_late_minutes',
    'Detention Rule': 'detention_rule',
    'Time In Yard (Hours)': 'csv_time_in_yard_hours',
    'Trailer #': 'trailer_number',
    'Trailer Number': 'trailer_number',
    'Trailer Type': 'trailer_type',
    'SCAC': 'scac',
    'Live/Drop': 'live_load',
    'Carrier SCAC': 'carrier_scac',
    'Facility': 'facility',
    'FACILITY': 'facility',
    'Fac Code': 'facility',
    'Fac_Code': 'facility',
    // Team data dump mappings
    'move_number': 'appointment_number',
    'trailer_number': 'trailer_number',
    'scac': 'scac',
    'reference_type_value_1': 'reference_1',
    'reference_type_value_2': 'reference_2',
    'comments': 'comments',
    'pre_detention_start_time': 'csv_predetention_combined',
    'detention_rule': 'detention_rule',
    'appointment_date': 'csv_appt_date',
    'appointment_time': 'csv_appt_time',
    'arrival_time_date': 'csv_arrival_date',
    'arrival_time_time': 'csv_arrival_time',
    'departure_date_date': 'csv_departure_date',
    'departure_date_time': 'csv_departure_time',
    'detention_start_date': 'csv_detention_date',
    'detention_start_times': 'csv_detention_time',
    'appt_early_late_status': 'csv_early_late',
    'appt_early_late_hours': 'csv_early_late_minutes',
    'total_elapsed_hours_in_yard': 'csv_time_in_yard_hours',
    'als_max_row_process_date_completed': 'csv_process_complete_date',
    'als_max_row_process_time_completed': 'csv_process_complete_time',
    'als_max_row_process_complete_hours': 'process_time_minutes',
    'move_type_name': 'move_type_name',
    'customer_name': 'customer_name',
    'facility_name': 'facility',
    'sub_load_type_name': 'sub_load_type_name',
    'trailer_type_name': 'trailer_type_name',
    'load_status_name': 'load_status_name',
    'trailer_condition_name': 'trailer_condition_name',
    'live_drop': 'live_load',
    'dwell_end_datetime': 'dwell_end_datetime',
    'empty_datetime': 'empty_datetime',
    // Additional standard column aliases
    'Appt #': 'appointment_number',
    'Ref 1': 'reference_1',
    'Ref 2': 'reference_2',
    'Comments': 'comments',
    'Appt Type': 'move_type_name',
    'Customer': 'customer_name',
    'Sub Load Type': 'sub_load_type_name',
    'Trailer Status': 'load_status_name',
    'Trailer Condition': 'trailer_condition_name',
    'Process Time (Minutes)': 'process_time_minutes',
    'Dwell End Datetime': 'dwell_end_datetime',
    'Empty Datetime': 'empty_datetime',
  },

  dockdoor_history: {
    // Existing standard column mappings
    'Date': 'csv_date',
    'Location': 'dock_door',
    'Dwell Start Date': 'csv_dwell_start_date',
    'Dwell Start Time': 'csv_dwell_start_time',
    'Dwell End Date': 'csv_dwell_end_date',
    'Dwell End Time': 'csv_dwell_end_time',
    'Dwell Time': 'csv_dwell_time_precomputed',
    'Process Start Date': 'csv_process_start_date',
    'Process Start Time': 'csv_process_start_time',
    'Process End Date': 'csv_process_end_date',
    'Process End Time': 'csv_process_end_time',
    'Process Time Minutes': 'csv_process_time_precomputed',
    'Processed By': 'processed_by',
    'Event': 'event',
    'Facility': 'facility',
    'FACILITY': 'facility',
    'Fac Code': 'facility',
    'Fac_Code': 'facility',
    // Team data dump mappings
    'date': 'csv_date',
    'location_name': 'dock_door',
    'facility_name': 'facility',
    'customer': 'customer_name',
    'load_status': 'load_status',
    'scac': 'scac',
    'trailer_number': 'trailer_number',
    'dwell_start_date': 'csv_dwell_start_date',
    'dwell_start_time': 'csv_dwell_start_time',
    'dwell_end_date': 'csv_dwell_end_date',
    'dwell_end_time': 'csv_dwell_end_time',
    'dwell_time': 'csv_dwell_time_precomputed',
    'process_start_date': 'csv_process_start_date',
    'process_start_time': 'csv_process_start_time',
    'process_end_date': 'csv_process_end_date',
    'process_end_time': 'csv_process_end_time',
    'process_time': 'csv_process_time_precomputed',
    'processed_by_name': 'processed_by',
    'appointment_number': 'appointment_number',
    'move_type_name': 'move_type_name',
    'trailer_type_name': 'trailer_type_name',
    'load_type_name': 'load_type_name',
    'sub_load_type_name': 'sub_load_type_name',
    'load_quantity_1': 'load_quantity',
    'reference_type_value_1': 'reference_1',
    'comments': 'comments',
    'arrival_time': 'arrival_time',
    'departure_date': 'departure_time',
    'event': 'event',
    'username': 'username',
    'move_requested_by': 'move_requested_by',
    // Additional standard column aliases
    'Customer': 'customer_name',
    'Load Status': 'load_status',
    'Carrier/SCAC': 'scac',
    'Trailer Number': 'trailer_number',
    'Appt #': 'appointment_number',
    'Appt Type': 'move_type_name',
    'Trailer Type': 'trailer_type_name',
    'Load Type': 'load_type_name',
    'Sub Load Type': 'sub_load_type_name',
    'Load Qty': 'load_quantity',
    'Ref 1': 'reference_1',
    'Comments': 'comments',
    'Arrival Time': 'arrival_time',
    'Departure Time': 'departure_time',
    'Username': 'username',
    'Move Requested By': 'move_requested_by',
  },

  driver_history: {
    // Existing standard column mappings
    'Date': 'csv_date',
    'Driver': 'yard_driver_name',
    'Carrier Company': 'csv_carrier_company',
    'Scac': 'scac',
    'SCAC': 'scac',
    'Trailer #': 'trailer_number',
    'Trailer Number': 'trailer_number',
    'Request Time': 'csv_request_time',
    'Time In Queue (Minutes)': 'time_in_queue_minutes',
    'Accept Time': 'csv_accept_time',
    'Start Time': 'csv_start_time',
    'Complete Time': 'csv_complete_time',
    'Elapsed Time (Minutes)': 'elapsed_time_minutes',
    'Event': 'event',
    'Facility': 'facility',
    'FACILITY': 'facility',
    'Fac Code': 'facility',
    'Fac_Code': 'facility',
    // Team data dump mappings
    'facility': 'facility',
    'created_at_date': 'csv_date',
    'created_at_time': 'csv_request_time',
    'accept_time': 'csv_accept_time',
    'start_time': 'csv_start_time',
    'complete_time': 'csv_complete_time',
    'priority_load_name': 'priority_load_name',
    'time_in_queue_minutes': 'time_in_queue_minutes',
    'elapsed_time_minutes': 'elapsed_time_minutes',
    'yard_driver_name': 'yard_driver_name',
    'appointment_number': 'appointment_number',
    'carrier_company': 'csv_carrier_company',
    'scac': 'scac',
    'trailer_plate': 'trailer_plate',
    'trailer_number': 'trailer_number',
    'vehicle_type': 'vehicle_type',
    'appt_comments': 'appt_comments',
    'move_request_comments': 'move_request_comments',
    'start_location_name': 'start_location_name',
    'start_spot': 'start_spot',
    'end_location_name': 'end_location_name',
    'spot': 'end_spot',
    'event': 'event',
    'requested_by': 'requested_by',
    'cancelled_by': 'cancelled_by',
    'cancelled_at': 'cancelled_at',
    'priority': 'priority_move',
    'priority_load': 'priority_load',
    'username': 'username',
    'customer_name': 'customer_name',
    'load_type_name': 'load_type_name',
    'declined_reason': 'declined_reason',
    // Additional standard column aliases
    'Appt#': 'appointment_number',
    'Trailer Plate': 'trailer_plate',
    'Vehicle Type': 'vehicle_type',
    'Comments': 'appt_comments',
    'Move Comments': 'move_request_comments',
    'Start Location': 'start_location_name',
    'Start Spot': 'start_spot',
    'End Location': 'end_location_name',
    'End Spot': 'end_spot',
    'Requested By': 'requested_by',
    'Cancelled By': 'cancelled_by',
    'Cancelled Time': 'cancelled_at',
    'Priority Move': 'priority_move',
    'Priority Load': 'priority_load_name',
    'Username': 'username',
    'Customer': 'customer_name',
    'Load Type': 'load_type_name',
    'Decline Reason': 'declined_reason',
  },

  trailer_history: {
    // Existing standard column mappings
    'Date': 'csv_date',
    'Time': 'csv_time',
    'Arrival Time': 'arrival_time',
    'Event': 'event',
    'SCAC': 'scac',
    'Trailer #': 'trailer_number',
    'Trailer Number': 'trailer_number',
    'Trailer Status': 'trailer_status',
    'Username': 'username',
    'Carrier SCAC': 'carrier_scac',
    'Start Location': 'start_location',
    'Facility': 'facility',
    'FACILITY': 'facility',
    'Fac Code': 'facility',
    'Fac_Code': 'facility',
    // Team data dump mappings (Trailer Event Log)
    'created_at_date': 'csv_date',
    'created_at_time': 'csv_time',
    'arrival_time': 'arrival_time',
    'trailer_condition_name': 'trailer_condition_name',
    'customer_name': 'customer_name',
    'appointment_number': 'appointment_number',
    'start_location': 'start_location',
    'scac': 'scac',
    'trailer_number': 'trailer_number',
    'origin': 'origin',
    'carrier_tractor': 'carrier_tractor',
    'reference_type_value_1': 'reference_1',
    'reference_type_value_2': 'reference_2',
    'move_type_name': 'move_type_name',
    'load_status_name': 'trailer_status',
    'load_type_name': 'load_type_name',
    'sub_load_type_name': 'sub_load_type_name',
    'load_quantity_1': 'load_quantity',
    'trailer_type_name': 'trailer_type_name',
    'drop_facility_name': 'facility',
    'event': 'event',
    'username': 'username',
    'live': 'live',
    'appointment_id': 'appointment_id',
    'ds2_is_lost': 'ds2_is_lost',
    'drop_spot_name_end': 'end_location',
    'carrier_driver_spotter': 'carrier_driver_spotter',
    // Additional standard column aliases
    'Trailer Condition': 'trailer_condition_name',
    'Customer': 'customer_name',
    'Appt #': 'appointment_number',
    'Origin/Destination': 'origin',
    'Tractor #': 'carrier_tractor',
    'Ref #1': 'reference_1',
    'Ref #2': 'reference_2',
    'Appt Type': 'move_type_name',
    'Load Type': 'load_type_name',
    'Sub Load Type': 'sub_load_type_name',
    'Load Qty': 'load_quantity',
    'Trailer Type': 'trailer_type_name',
    'Live': 'live',
    'End Location': 'end_location',
  },
};

/**
 * Signature columns used to auto-detect report types.
 * A report is detected if ALL signature columns are present.
 */
export const REPORT_SIGNATURES = {
  current_inventory: ['Trailer Number', 'Move Type', 'Load Status'],
  detention_history: ['Detention Date', 'Detention Rule', 'Time In Yard'],
  dockdoor_history: ['Dwell Start Date', 'Process Start Date', 'Processed By'],
  driver_history: ['Driver', 'Request Time', 'Accept Time', 'Complete Time'],
  trailer_history: ['Trailer #', 'Trailer Status', 'Event'],
};

/**
 * Alternative signatures for more flexible detection
 * Includes both standard report formats and team data dump formats
 */
const REPORT_SIGNATURES_ALT = {
  current_inventory: [
    ['Trailer Number', 'SCAC', 'Move Type'],
    ['Trailer Number', 'Load Status'],
    // Team data dump format
    ['trailer_number', 'move_type_name', 'load_status_name'],
    ['move_number', 'trailer_number', 'scac'],
    ['trailer_number', 'move_type_name', 'drop_facility_name'],
  ],
  detention_history: [
    ['Detention Date', 'Detention Time'],
    ['SCAC', 'Live/Drop', 'Time In Yard'],
    // Team data dump format
    ['detention_start_date', 'detention_rule', 'total_elapsed_hours_in_yard'],
    ['move_number', 'trailer_number', 'detention_rule'],
    ['scac', 'live_drop', 'total_elapsed_hours_in_yard'],
    ['pre_detention_start_time', 'detention_rule'],
  ],
  dockdoor_history: [
    ['Dwell Start Time', 'Process Start Time'],
    ['Processed By', 'Dwell Time'],
    // Team data dump format
    ['dwell_start_date', 'process_start_date', 'processed_by_name'],
    ['location_name', 'dwell_time', 'process_time'],
    ['dwell_start_time', 'process_start_time', 'processed_by_name'],
  ],
  driver_history: [
    ['Driver', 'Request Time'],
    ['Driver', 'Elapsed Time (Minutes)'],
    // Team data dump format
    ['yard_driver_name', 'created_at_time', 'accept_time', 'complete_time'],
    ['yard_driver_name', 'time_in_queue_minutes', 'elapsed_time_minutes'],
    ['facility', 'yard_driver_name', 'event'],
  ],
  trailer_history: [
    ['Trailer #', 'Event'],
    ['Trailer Status', 'Username'],
    // Team data dump format (Trailer Event Log)
    ['trailer_number', 'event', 'load_status_name'],
    ['created_at_date', 'created_at_time', 'trailer_number', 'event'],
    ['trailer_number', 'username', 'event'],
  ],
};

// ---------- Date/Time Parsing ----------

/**
 * Combines separate date and time columns into a single timestamp string.
 * @param {string} dateStr - Date in MM-DD-YYYY format
 * @param {string} timeStr - Time in HH:mm format (optional)
 * @returns {string|null} Combined timestamp string or null
 */
export function combineDateTimeColumns(dateStr, timeStr) {
  if (!dateStr || dateStr.trim() === '') return null;
  const date = dateStr.trim();
  const time = timeStr && timeStr.trim() !== '' ? timeStr.trim() : '00:00';
  return `${date} ${time}`;
}

/**
 * Normalizes boolean-like values from CSV.
 * @param {*} value - The value to normalize
 * @returns {boolean|null} Normalized boolean or null
 */
function normalizeBoolish(value) {
  if (typeof value === 'boolean') return value;
  if (value === null || value === undefined || value === '') return null;
  const s = String(value).toLowerCase().trim();
  if (s === '1' || s === 'true' || s === 'yes' || s === 'y' || s === 'live') return true;
  if (s === '0' || s === 'false' || s === 'no' || s === 'n' || s === 'drop') return false;
  return null;
}

// ---------- Report Type Detection ----------

/**
 * Auto-detects the report type based on CSV column headers.
 * @param {string[]} columns - Array of column headers from the CSV
 * @returns {string|null} Report type or null if unknown
 */
export function detectReportType(columns) {
  if (!columns || !Array.isArray(columns)) return null;

  const columnSet = new Set(columns.map(c => c.trim()));

  // Try primary signatures first
  for (const [report, signature] of Object.entries(REPORT_SIGNATURES)) {
    if (signature.every(col => columnSet.has(col))) {
      return report;
    }
  }

  // Try alternative signatures
  for (const [report, altSigs] of Object.entries(REPORT_SIGNATURES_ALT)) {
    for (const sig of altSigs) {
      if (sig.every(col => columnSet.has(col))) {
        return report;
      }
    }
  }

  // Partial matching with scoring
  let bestMatch = null;
  let bestScore = 0;

  for (const [report, signature] of Object.entries(REPORT_SIGNATURES)) {
    const matchCount = signature.filter(col => columnSet.has(col)).length;
    const score = matchCount / signature.length;
    if (score > bestScore && score >= 0.6) {
      bestScore = score;
      bestMatch = report;
    }
  }

  return bestMatch;
}

// ---------- Facility Extraction ----------

/**
 * Extracts the facility identifier from a CSV row based on report type.
 * Used to support multi-facility analysis and comparison features.
 *
 * @param {object} normalizedRow - The normalized row object (after field mapping)
 * @param {object} rawRow - The original raw CSV row (for unmapped fields)
 * @param {string} reportType - The report type
 * @returns {string} The facility identifier, or empty string if not found
 */
function extractFacilityFromRow(normalizedRow, rawRow, reportType) {
  let facility = '';

  switch (reportType) {
    case 'dockdoor_history':
    case 'driver_history':
    case 'detention_history':
      // Direct Facility field with multiple fallback options for different CSV formats
      facility = normalizedRow.facility
        || rawRow.Facility
        || rawRow.facility
        || rawRow.FACILITY
        || rawRow['Fac Code']
        || rawRow.fac_code
        || rawRow.FacCode
        || '';
      break;

    case 'trailer_history':
      // Extract from Start Location - text before first " -" (space-dash)
      // e.g., "Central Parking - South Doors - Gate" → "Central Parking"
      const startLoc = normalizedRow.start_location || rawRow['Start Location'] || rawRow.start_location || '';
      if (startLoc) {
        const parts = startLoc.split(' -');
        facility = parts[0] || '';
      }
      // Fallback to direct facility field if start location extraction failed
      if (!facility) {
        facility = normalizedRow.facility
          || rawRow.Facility
          || rawRow.facility
          || rawRow.FACILITY
          || '';
      }
      break;

    case 'current_inventory':
      // Use Drop Facility field with fallbacks
      facility = normalizedRow.drop_facility
        || rawRow['Drop Facility']
        || rawRow.drop_facility
        || normalizedRow.facility
        || rawRow.Facility
        || rawRow.FACILITY
        || '';
      break;

    default:
      facility = '';
  }

  return (facility || '').trim();
}

// ---------- Field Normalization ----------

/**
 * Normalizes a single CSV row to API-expected field names.
 * Also handles value transformations (booleans, combined timestamps).
 *
 * @param {object} row - Raw CSV row object
 * @param {string} reportType - The report type
 * @param {string} timezone - Timezone for timestamp interpretation
 * @returns {object} Normalized row object
 */
export function normalizeCSVRow(row, reportType, timezone) {
  const map = CSV_FIELD_MAPS[reportType];
  if (!map) return row; // Return as-is if no mapping defined

  const normalized = {};

  // Map known columns
  for (const [csvCol, apiField] of Object.entries(map)) {
    if (row[csvCol] !== undefined && row[csvCol] !== null && row[csvCol] !== '') {
      normalized[apiField] = row[csvCol];
    }
  }

  // Copy unmapped columns as-is (preserve original data)
  for (const key of Object.keys(row)) {
    if (!Object.keys(map).includes(key)) {
      // Use snake_case for unmapped columns
      const snakeKey = key.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
      if (!normalized[snakeKey]) {
        normalized[snakeKey] = row[key];
      }
    }
  }

  // Report-specific transformations
  switch (reportType) {
    case 'current_inventory':
      normalizeCurrentInventory(normalized, row);
      break;
    case 'detention_history':
      normalizeDetentionHistory(normalized, row);
      break;
    case 'dockdoor_history':
      normalizeDockdoorHistory(normalized, row);
      break;
    case 'driver_history':
      normalizeDriverHistory(normalized, row);
      break;
    case 'trailer_history':
      normalizeTrailerHistory(normalized, row);
      break;
  }

  // Extract facility identifier for multi-facility support
  // Uses underscore prefix to indicate internal/derived field
  normalized._facility = extractFacilityFromRow(normalized, row, reportType);

  return normalized;
}

/**
 * Normalizes current_inventory specific fields
 */
function normalizeCurrentInventory(normalized, row) {
  // Convert live_load to boolean
  if (normalized.live_load !== undefined) {
    normalized.live_load = normalizeBoolish(normalized.live_load);
  }

  // Synthesize updated_at from Elapsed Time (Hours) if missing
  if (!normalized.updated_at && normalized.csv_elapsed_hours) {
    const hours = parseFloat(normalized.csv_elapsed_hours);
    if (Number.isFinite(hours)) {
      const now = Date.now();
      const ageMs = hours * 60 * 60 * 1000;
      normalized.updated_at = new Date(now - ageMs).toISOString();
    }
  }

  // Use Latest Loaded Time as updated_at proxy if available
  if (!normalized.updated_at && normalized.csv_latest_loaded) {
    normalized.updated_at = normalized.csv_latest_loaded;
  }
}

/**
 * Normalizes detention_history specific fields
 */
function normalizeDetentionHistory(normalized, row) {
  // Convert live_load (Live/Drop)
  if (normalized.live_load !== undefined) {
    normalized.live_load = normalizeBoolish(normalized.live_load);
  }

  // Handle combined pre_detention_start_time field from team data dump
  // Format: "2026-02-26 07:03:50" - needs to be split into date and time
  if (normalized.csv_predetention_combined && !normalized.csv_predetention_date) {
    const combined = normalized.csv_predetention_combined.trim();
    const spaceIndex = combined.indexOf(' ');
    if (spaceIndex > 0) {
      normalized.csv_predetention_date = combined.substring(0, spaceIndex);
      normalized.csv_predetention_time = combined.substring(spaceIndex + 1);
    } else {
      // If no space, treat entire value as date
      normalized.csv_predetention_date = combined;
    }
  }

  // Combine date/time columns into timestamps
  if (normalized.csv_appt_date) {
    normalized.appointment_time = combineDateTimeColumns(
      normalized.csv_appt_date,
      normalized.csv_appt_time
    );
  }

  if (normalized.csv_arrival_date) {
    normalized.arrival_time = combineDateTimeColumns(
      normalized.csv_arrival_date,
      normalized.csv_arrival_time
    );
  }

  if (normalized.csv_predetention_date) {
    normalized.pre_detention_start_time = combineDateTimeColumns(
      normalized.csv_predetention_date,
      normalized.csv_predetention_time
    );
  }

  if (normalized.csv_detention_date) {
    normalized.detention_start_time = combineDateTimeColumns(
      normalized.csv_detention_date,
      normalized.csv_detention_time
    );
  }

  if (normalized.csv_departure_date) {
    normalized.departure_datetime = combineDateTimeColumns(
      normalized.csv_departure_date,
      normalized.csv_departure_time
    );
  }

  if (normalized.csv_process_complete_date) {
    normalized.process_complete_time = combineDateTimeColumns(
      normalized.csv_process_complete_date,
      normalized.csv_process_complete_time
    );
  }
}

/**
 * Normalizes dockdoor_history specific fields
 */
function normalizeDockdoorHistory(normalized, row) {
  // Combine dwell start date/time
  if (normalized.csv_dwell_start_date) {
    normalized.dwell_start_time = combineDateTimeColumns(
      normalized.csv_dwell_start_date,
      normalized.csv_dwell_start_time
    );
  }

  // Combine dwell end date/time
  if (normalized.csv_dwell_end_date) {
    normalized.dwell_end_time = combineDateTimeColumns(
      normalized.csv_dwell_end_date,
      normalized.csv_dwell_end_time
    );
  }

  // Combine process start date/time
  if (normalized.csv_process_start_date) {
    normalized.process_start_time = combineDateTimeColumns(
      normalized.csv_process_start_date,
      normalized.csv_process_start_time
    );
  }

  // Combine process end date/time
  if (normalized.csv_process_end_date) {
    normalized.process_end_time = combineDateTimeColumns(
      normalized.csv_process_end_date,
      normalized.csv_process_end_time
    );
  }
}

/**
 * Normalizes driver_history specific fields
 */
function normalizeDriverHistory(normalized, row) {
  const baseDate = normalized.csv_date;

  // Combine date with time fields
  if (baseDate) {
    if (normalized.csv_request_time) {
      normalized.request_time = combineDateTimeColumns(baseDate, normalized.csv_request_time);
    }
    if (normalized.csv_accept_time) {
      normalized.accept_time = combineDateTimeColumns(baseDate, normalized.csv_accept_time);
    }
    if (normalized.csv_start_time) {
      normalized.start_time = combineDateTimeColumns(baseDate, normalized.csv_start_time);
    }
    if (normalized.csv_complete_time) {
      normalized.complete_time = combineDateTimeColumns(baseDate, normalized.csv_complete_time);
    }
  }

  // Normalize numeric fields
  if (normalized.time_in_queue_minutes !== undefined) {
    normalized.time_in_queue_minutes = parseFloat(normalized.time_in_queue_minutes) || null;
  }
  if (normalized.elapsed_time_minutes !== undefined) {
    normalized.elapsed_time_minutes = parseFloat(normalized.elapsed_time_minutes) || null;
  }
}

/**
 * Normalizes trailer_history specific fields
 */
function normalizeTrailerHistory(normalized, row) {
  const baseDate = normalized.csv_date;
  const baseTime = normalized.csv_time;

  // Combine date + time into event_time
  if (baseDate) {
    normalized.event_time = combineDateTimeColumns(baseDate, baseTime);
  }
}

// ---------- CSV Validation ----------

/**
 * Validates CSV data for a given report type.
 * Returns warnings about missing columns, format issues, etc.
 *
 * @param {string[]} columns - CSV column headers
 * @param {string} reportType - The report type
 * @returns {object} { isValid: boolean, warnings: string[] }
 */
export function validateCSVColumns(columns, reportType) {
  const warnings = [];
  const map = CSV_FIELD_MAPS[reportType];

  if (!map) {
    warnings.push(`Unknown report type: ${reportType}`);
    return { isValid: false, warnings };
  }

  const expectedCols = Object.keys(map);
  const presentCols = new Set(columns);
  const missingCols = expectedCols.filter(col => !presentCols.has(col));

  // Check for critical missing columns
  const criticalMissing = missingCols.filter(col => {
    const apiField = map[col];
    // Define critical fields per report
    const critical = {
      current_inventory: ['trailer_number', 'move_type_name'],
      detention_history: ['scac'],
      dockdoor_history: ['processed_by'],
      driver_history: ['yard_driver_name'],
      trailer_history: ['trailer_number', 'event'],
    };
    return critical[reportType]?.includes(apiField);
  });

  if (criticalMissing.length > 0) {
    warnings.push(`Missing critical columns: ${criticalMissing.join(', ')}`);
  }

  // Warn about optional missing columns
  const optionalMissing = missingCols.filter(col => !criticalMissing.includes(col));
  if (optionalMissing.length > 0 && optionalMissing.length <= 5) {
    warnings.push(`Missing optional columns: ${optionalMissing.join(', ')}`);
  } else if (optionalMissing.length > 5) {
    warnings.push(`Missing ${optionalMissing.length} optional columns`);
  }

  return {
    isValid: criticalMissing.length === 0,
    warnings,
  };
}

/**
 * Validates a single row of CSV data.
 * @param {object} row - Normalized row object
 * @param {string} reportType - The report type
 * @param {number} rowIndex - Row index for error messages
 * @returns {string[]} Array of warning messages (empty if valid)
 */
export function validateCSVRow(row, reportType, rowIndex) {
  const warnings = [];

  // Check for empty critical fields
  const criticalFields = {
    current_inventory: ['trailer_number'],
    detention_history: ['scac'],
    dockdoor_history: [],
    driver_history: ['yard_driver_name'],
    trailer_history: ['event'],
  };

  const fields = criticalFields[reportType] || [];
  for (const field of fields) {
    if (!row[field] || row[field] === '') {
      warnings.push(`Row ${rowIndex + 1}: Missing required field "${field}"`);
    }
  }

  return warnings;
}

// ---------- CSV Parsing (Papa Parse wrapper) ----------

/**
 * Parses CSV text using Papa Parse.
 * @param {string} csvText - Raw CSV text
 * @param {object} options - Parsing options
 * @returns {Promise<{data: object[], columns: string[], errors: any[]}>}
 */
export function parseCSVText(csvText, options = {}) {
  return new Promise((resolve, reject) => {
    if (typeof Papa === 'undefined') {
      reject(new Error('Papa Parse library not loaded'));
      return;
    }

    Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false, // Keep all values as strings for consistent handling
      transformHeader: (header) => header.trim(),
      complete: (results) => {
        resolve({
          data: results.data,
          columns: results.meta.fields || [],
          errors: results.errors,
        });
      },
      error: (error) => {
        reject(error);
      },
      ...options,
    });
  });
}

/**
 * Parses a CSV file and returns parsed data.
 * @param {File} file - File object from file input
 * @param {object} options - Parsing options
 * @returns {Promise<{data: object[], columns: string[], errors: any[]}>}
 */
export function parseCSVFile(file, options = {}) {
  return new Promise((resolve, reject) => {
    if (typeof Papa === 'undefined') {
      reject(new Error('Papa Parse library not loaded'));
      return;
    }

    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      transformHeader: (header) => header.trim(),
      complete: (results) => {
        resolve({
          data: results.data,
          columns: results.meta.fields || [],
          errors: results.errors,
        });
      },
      error: (error) => {
        reject(error);
      },
      ...options,
    });
  });
}

/**
 * Streams a CSV file for large dataset processing.
 * Yields normalized rows in chunks for memory efficiency.
 *
 * @param {File} file - File object
 * @param {string} reportType - Report type for normalization
 * @param {string} timezone - Timezone for timestamp parsing
 * @param {object} callbacks - { onProgress, onChunk, onComplete, onError }
 * @param {number} chunkSize - Number of rows per chunk (default 500)
 * @param {number|null} knownRowCount - Known row count from preview parse (optional)
 */
export function streamCSVFile(file, reportType, timezone, callbacks, chunkSize = 500, knownRowCount = null) {
  const { onProgress, onChunk, onComplete, onError } = callbacks;

  if (typeof Papa === 'undefined') {
    onError?.(new Error('Papa Parse library not loaded'));
    return;
  }

  let rowsProcessed = 0;
  let chunk = [];
  let columns = [];

  // Use known row count if available, otherwise estimate from file size
  const totalEstimate = knownRowCount || Math.ceil(file.size / 100);

  Papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
    transformHeader: (header) => header.trim(),
    step: (results, parser) => {
      if (columns.length === 0) {
        columns = results.meta.fields || [];
      }

      const normalizedRow = normalizeCSVRow(results.data, reportType, timezone);
      chunk.push(normalizedRow);
      rowsProcessed++;

      if (chunk.length >= chunkSize) {
        onChunk?.(chunk, rowsProcessed);
        onProgress?.(rowsProcessed, totalEstimate);
        chunk = [];
      }
    },
    complete: () => {
      // Process remaining chunk
      if (chunk.length > 0) {
        onChunk?.(chunk, rowsProcessed);
      }
      onProgress?.(rowsProcessed, rowsProcessed);
      onComplete?.({
        totalRows: rowsProcessed,
        columns,
      });
    },
    error: (error) => {
      onError?.(error);
    },
  });
}

// ---------- Yard Age Buckets (CSV-specific feature) ----------

/**
 * Computes yard age bucket from elapsed hours.
 * @param {number} elapsedHours - Hours in yard
 * @returns {string} Bucket label
 */
export function computeYardAgeBucket(elapsedHours) {
  if (!Number.isFinite(elapsedHours) || elapsedHours < 0) return 'unknown';
  if (elapsedHours <= 24) return '0-1d';
  if (elapsedHours <= 168) return '1-7d';
  if (elapsedHours <= 720) return '7-30d';
  return '30d+';
}

/**
 * Aggregates yard age distribution from CSV elapsed hours data.
 * @param {object[]} rows - Normalized rows with csv_elapsed_hours
 * @returns {object} Bucket counts
 */
export function aggregateYardAgeBuckets(rows) {
  const buckets = {
    '0-1d': 0,
    '1-7d': 0,
    '7-30d': 0,
    '30d+': 0,
    'unknown': 0,
  };

  for (const row of rows) {
    const hours = parseFloat(row.csv_elapsed_hours);
    const bucket = computeYardAgeBucket(hours);
    buckets[bucket]++;
  }

  return buckets;
}
