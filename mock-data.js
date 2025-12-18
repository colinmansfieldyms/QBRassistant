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
        { dwell_start_time: '2025-11-01 10:00:00', dwell_end_time: '2025-11-01 11:20:00', process_start_time: '2025-11-01 10:10:00', process_end_time: '2025-11-01 11:00:00', processed_by: 'j.smith', move_requested_by: 'Admin' },
        { dwell_start_time: '2025-12-01 08:00:00', dwell_end_time: '2025-12-01 09:05:00', process_start_time: '2025-12-01 08:10:00', process_end_time: '2025-12-01 08:55:00', processed_by: 'a.lee', move_requested_by: 'Admin' },
      ],
    ],
    FAC2: [
      [
        { dwell_start_time: '2025-12-05 09:00:00', dwell_end_time: '2025-12-05 10:45:00', process_start_time: null, process_end_time: null, processed_by: 'a.lee', move_requested_by: 'ops.user' },
      ]
    ]
  },

  driver_history: {
    FAC1: [
      [
        { driver_name: 'Driver A', move_complete_time: '2025-12-10 10:10:00', move_accept_time: '2025-12-10 10:09:00', elapsed_time_minutes: 1.0, time_in_queue_minutes: 12 },
        { driver_name: 'Driver B', move_complete_time: '2025-12-10 10:30:00', move_accept_time: '2025-12-10 10:20:00', elapsed_time_minutes: 10.0, time_in_queue_minutes: 5 },
      ],
      [
        { driver_name: 'Driver A', move_complete_time: '2025-12-11 11:00:00', move_accept_time: '2025-12-11 10:58:30', elapsed_time_minutes: 1.5, time_in_queue_minutes: 8 },
      ]
    ],
    FAC2: [
      [
        { driver_name: 'Driver C', move_complete_time: '2025-12-12 12:10:00', move_accept_time: '2025-12-12 12:09:30', elapsed_time_minutes: 0.7, time_in_queue_minutes: 2 },
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

export function getMockPage({ report, facility, page }) {
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
