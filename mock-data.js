/**
 * Small sample payloads for demoing without hitting real API.
 * Shape matches API pagination: { current_page, last_page, next_page_url, data }.
 *
 * NOTE: PII rule still applies. Even in mock data, we include only null/not-null “driver phone” fields;
 * and analysis.js will scrub them anyway.
 */

export const MOCK_TIMEZONES = [
  'America/Los_Angeles',
  'America/Chicago',
  'America/New_York',
  'UTC'
];

const pages = {
  current_inventory: {
    FAC1: [
      [
        { trailer: 'T100', updated_at: '2025-12-16 18:20:00', scac: 'ABCD', move_type_name: 'Outbound', live_load: 1, driver_cell_number: null },
        { trailer: 'T101', updated_at: '2025-12-18 03:10:00', scac: 'UNKNOWN', move_type_name: 'Inbound', live_load: 0, driver_cell_number: 'present' },
      ],
      [
        { trailer: 'T102', updated_at: '2025-11-01 10:00:00', scac: 'WXYZ', move_type_name: 'Outbound', live_load: 1, driver_cell_number: null },
      ],
    ],
    FAC2: [
      [
        { trailer: 'T200', updated_at: '2025-12-18 04:00:00', scac: 'XXXX', move_type_name: 'Inbound', live_load: 1, driver_cell_number: 'present' },
      ],
    ]
  },

  detention_history: {
    FAC1: [
      [
        { pre_detention_start_time: '2025-10-01 12:00:00', detention_start_time: null, scac: 'ABCD', live_load: 1 },
        { pre_detention_start_time: '2025-10-02 12:00:00', detention_start_time: '2025-10-02 13:00:00', scac: 'ABCD', live_load: 1 },
      ],
    ],
    FAC2: [
      [
        { pre_detention_start_time: '09-15-2025 09:30', detention_start_time: null, scac: 'WXYZ', live_load: 0 },
      ],
    ]
  },

  dockdoor_history: {
    FAC1: [
      [
        { location: 'Door 1', dwell_start_time: '2025-11-01 10:00:00', dwell_end_time: '2025-11-01 11:20:00', process_start_time: '2025-11-01 10:10:00', process_end_time: '2025-11-01 11:00:00', processed_by: 'j.smith', move_requested_by: 'Admin' },
        { location: 'Door 2', dwell_start_time: '2025-12-01 08:00:00', dwell_end_time: '2025-12-01 09:05:00', process_start_time: '2025-12-01 08:10:00', process_end_time: '2025-12-01 08:55:00', processed_by: 'a.lee', move_requested_by: 'Admin' },
      ],
    ],
    FAC2: [
      [
        { location: 'Door A', dwell_start_time: '2025-12-05 09:00:00', dwell_end_time: '2025-12-05 10:45:00', process_start_time: null, process_end_time: null, processed_by: 'a.lee', move_requested_by: 'ops.user' },
      ]
    ]
  },

  driver_history: {
    FAC1: [
      [
        { yard_driver_name: 'Driver A', complete_time: '2025-12-10 10:10:00', move_accept_time: '2025-12-10 10:05:00', start_time: '2025-12-10 10:07:00', elapsed_time_minutes: 5.0, time_in_queue_minutes: 12, event: 'Move has been finished' },
        { yard_driver_name: 'Driver B', complete_time: '2025-12-10 10:30:00', move_accept_time: '2025-12-10 10:20:00', start_time: '2025-12-10 10:24:00', elapsed_time_minutes: 10.0, time_in_queue_minutes: 5, event: 'Move has been finished' },
      ],
      [
        { yard_driver_name: 'Driver A', complete_time: '2025-12-11 11:00:00', move_accept_time: '2025-12-11 10:52:00', start_time: '2025-12-11 10:55:00', elapsed_time_minutes: 8.0, time_in_queue_minutes: 8, event: 'Move has been finished' },
      ]
    ],
    FAC2: [
      [
        { yard_driver_name: 'Driver C', complete_time: '2025-12-12 12:10:00', move_accept_time: '2025-12-12 12:02:00', start_time: '2025-12-12 12:05:00', elapsed_time_minutes: 8.0, time_in_queue_minutes: 2, event: 'Move has been finished' },
      ]
    ]
  },

  trailer_history: {
    FAC1: [
      [
        { event: 'Trailer marked lost', created_at: '2025-12-01 09:00:00', scac: 'ABCD' },
        { event: 'Trailer check-in', created_at: '2025-12-01 10:00:00', scac: 'ABCD' },
      ],
      [
        { event: 'Trailer marked lost', created_at: '2025-12-08 09:00:00', scac: 'WXYZ' },
      ],
    ],
    FAC2: [
      [
        { event: 'Trailer marked lost', created_at: '2025-12-03 14:00:00', scac: 'WXYZ' },
      ],
    ]
  }
};

// Large dataset mode - enabled via ?largedata=N in URL (N = page count, default 100)
function generateLargeDatasetPage({ report, facility, page, totalPages, rowsPerPage = 50 }) {
  const data = [];
  const baseTime = new Date('2025-10-01T00:00:00Z').getTime();

  for (let i = 0; i < rowsPerPage; i++) {
    const rowIndex = (page - 1) * rowsPerPage + i;
    const timestamp = new Date(baseTime + rowIndex * 3600000).toISOString().replace('T', ' ').slice(0, 19);

    if (report === 'detention_history') {
      data.push({
        pre_detention_start_time: timestamp,
        detention_start_time: i % 3 === 0 ? timestamp : null,
        scac: ['ABCD', 'WXYZ', 'TEST'][i % 3],
        live_load: i % 2
      });
    } else if (report === 'current_inventory') {
      data.push({
        trailer: `T${rowIndex}`,
        updated_at: timestamp,
        scac: ['ABCD', 'WXYZ', 'TEST'][i % 3],
        move_type_name: i % 2 ? 'Outbound' : 'Inbound',
        live_load: i % 2,
        driver_cell_number: i % 4 === 0 ? 'present' : null
      });
    } else if (report === 'dockdoor_history') {
      const endTime = new Date(baseTime + rowIndex * 3600000 + 3600000).toISOString().replace('T', ' ').slice(0, 19);
      data.push({
        dwell_start_time: timestamp,
        dwell_end_time: endTime,
        process_start_time: i % 3 === 0 ? timestamp : null,
        process_end_time: i % 3 === 0 ? endTime : null,
        processed_by: ['j.smith', 'a.lee', 'b.jones'][i % 3],
        move_requested_by: 'Admin'
      });
    } else if (report === 'driver_history') {
      const acceptTime = new Date(baseTime + rowIndex * 3600000 - 600000);
      const startTime = new Date(acceptTime.getTime() + (2 + (i % 5)) * 60000); // 2-6 min after accept
      data.push({
        yard_driver_name: `Driver ${String.fromCharCode(65 + (i % 10))}`,
        complete_time: timestamp,
        move_accept_time: acceptTime.toISOString().replace('T', ' ').slice(0, 19),
        start_time: startTime.toISOString().replace('T', ' ').slice(0, 19),
        elapsed_time_minutes: 1 + (i % 20),
        time_in_queue_minutes: 5 + (i % 15),
        event: 'Move has been finished'
      });
    } else if (report === 'trailer_history') {
      data.push({
        event: ['Trailer marked lost', 'Trailer check-in', 'Trailer marked found'][i % 3],
        created_at: timestamp,
        scac: ['ABCD', 'WXYZ', 'TEST'][i % 3]
      });
    }
  }

  return {
    current_page: page,
    last_page: totalPages,
    next_page_url: page < totalPages ? `mock://next/${report}/${facility}/${page + 1}` : null,
    data
  };
}

export function getMockPage({ report, facility, page }) {
  // Check for large dataset mode
  const params = new URLSearchParams(window.location.search);
  const largeDataPages = parseInt(params.get('largedata'), 10);

  if (largeDataPages > 0 && largeDataPages <= 5000) {
    return generateLargeDatasetPage({ report, facility, page, totalPages: largeDataPages });
  }

  // Normal mock mode
  const fac = pages?.[report]?.[facility];
  const data = fac?.[page - 1] || [];
  const last_page = fac ? fac.length : 1;
  return {
    current_page: page,
    last_page,
    next_page_url: page < last_page ? `mock://next/${report}/${facility}/${page + 1}` : null,
    data
  };
}
