/**
 * Backpressure Configuration Override System
 *
 * This module manages runtime overrides for backpressure settings,
 * allowing advanced users to experiment with different configurations
 * for testing and debugging purposes.
 */

// ============================================================================
// SYSTEM DEFAULTS (read-only reference)
// These match the hardcoded values in api.js, worker-transfer.js, etc.
// ============================================================================

export const SYSTEM_DEFAULTS = Object.freeze({
  // Concurrency
  globalMaxConcurrency: 8,

  // Green Zone
  greenZoneEnabled: true,
  greenZoneConcurrencyMax: 12,
  greenZoneStreakCount: 4,

  // Pipeline
  fetchBufferSize: 9,
  processingPoolSize: 3,
  pageQueueLimit: 10,

  // Dataset Tier
  forceTier: 'auto', // 'auto' | 'tiny' | 'small' | 'medium' | 'large' | 'huge'

  // Worker
  batchSize: 600,
  partialUpdateInterval: 1000,
});

// ============================================================================
// TIER DEFINITIONS
// ============================================================================

export const TIER_CONFIGS = Object.freeze({
  auto: null, // Use automatic tier selection based on page count
  tiny: { maxPages: 50, maxInFlight: 8, yieldEvery: 2, fetchBuffer: 10, processingMax: 4 },
  small: { maxPages: 200, maxInFlight: 6, yieldEvery: 1, fetchBuffer: 8, processingMax: 4 },
  medium: { maxPages: 500, maxInFlight: 4, yieldEvery: 1, fetchBuffer: 6, processingMax: 3 },
  large: { maxPages: 1000, maxInFlight: 3, yieldEvery: 1, fetchBuffer: 5, processingMax: 3 },
  huge: { maxPages: Infinity, maxInFlight: 2, yieldEvery: 1, fetchBuffer: 4, processingMax: 2 },
});

// ============================================================================
// PRESETS
// ============================================================================

export const PRESETS = Object.freeze({
  'production-safe': {
    name: 'Production Safe',
    description: 'Current system defaults - balanced for safety and performance',
    config: { ...SYSTEM_DEFAULTS },
  },
  'speed-demon': {
    name: 'Speed Demon',
    description: 'Maximum throughput - may cause UI lag on slower devices',
    config: {
      globalMaxConcurrency: 16,
      greenZoneEnabled: true,
      greenZoneConcurrencyMax: 20,
      greenZoneStreakCount: 2,
      fetchBufferSize: 15,
      processingPoolSize: 6,
      pageQueueLimit: 20,
      forceTier: 'auto',
      batchSize: 1200,
      partialUpdateInterval: 500,
    },
  },
  'stress-test': {
    name: 'Stress Test',
    description: 'Very high concurrency with tiny buffers - finds breaking points',
    config: {
      globalMaxConcurrency: 20,
      greenZoneEnabled: true,
      greenZoneConcurrencyMax: 20,
      greenZoneStreakCount: 1,
      fetchBufferSize: 2,
      processingPoolSize: 1,
      pageQueueLimit: 30,
      forceTier: 'auto',
      batchSize: 200,
      partialUpdateInterval: 200,
    },
  },
  'low-memory': {
    name: 'Low Memory Device',
    description: 'Minimal buffers and concurrency - simulates constrained devices',
    config: {
      globalMaxConcurrency: 3,
      greenZoneEnabled: false,
      greenZoneConcurrencyMax: 4,
      greenZoneStreakCount: 6,
      fetchBufferSize: 2,
      processingPoolSize: 1,
      pageQueueLimit: 5,
      forceTier: 'huge',
      batchSize: 200,
      partialUpdateInterval: 2000,
    },
  },
  'bad-network': {
    name: 'Bad Network',
    description: 'Low concurrency with large buffers - simulates high latency',
    config: {
      globalMaxConcurrency: 2,
      greenZoneEnabled: false,
      greenZoneConcurrencyMax: 4,
      greenZoneStreakCount: 8,
      fetchBufferSize: 12,
      processingPoolSize: 4,
      pageQueueLimit: 15,
      forceTier: 'auto',
      batchSize: 800,
      partialUpdateInterval: 1500,
    },
  },
  'no-green-zone': {
    name: 'No Green Zone',
    description: 'Defaults with Green Zone disabled - A/B test the optimization',
    config: {
      ...SYSTEM_DEFAULTS,
      greenZoneEnabled: false,
    },
  },
  'conservative': {
    name: 'Conservative',
    description: 'Half the defaults across the board - extra safe mode',
    config: {
      globalMaxConcurrency: 4,
      greenZoneEnabled: true,
      greenZoneConcurrencyMax: 6,
      greenZoneStreakCount: 6,
      fetchBufferSize: 5,
      processingPoolSize: 2,
      pageQueueLimit: 5,
      forceTier: 'auto',
      batchSize: 300,
      partialUpdateInterval: 1500,
    },
  },
  'large-dataset': {
    name: 'Large Dataset',
    description: 'Forces "huge" tier settings regardless of actual size',
    config: {
      ...SYSTEM_DEFAULTS,
      forceTier: 'huge',
    },
  },
  'rapid-feedback': {
    name: 'Rapid Feedback',
    description: 'Small batches with fast UI updates - debug data flow',
    config: {
      globalMaxConcurrency: 6,
      greenZoneEnabled: true,
      greenZoneConcurrencyMax: 10,
      greenZoneStreakCount: 4,
      fetchBufferSize: 4,
      processingPoolSize: 2,
      pageQueueLimit: 8,
      forceTier: 'auto',
      batchSize: 150,
      partialUpdateInterval: 200,
    },
  },
  'throughput-focus': {
    name: 'Throughput Focus',
    description: 'Large batches with slow UI updates - maximize processing speed',
    config: {
      globalMaxConcurrency: 10,
      greenZoneEnabled: true,
      greenZoneConcurrencyMax: 14,
      greenZoneStreakCount: 3,
      fetchBufferSize: 12,
      processingPoolSize: 4,
      pageQueueLimit: 15,
      forceTier: 'auto',
      batchSize: 1500,
      partialUpdateInterval: 3000,
    },
  },
});

// ============================================================================
// CONTROL DEFINITIONS (for UI generation)
// ============================================================================

export const CONTROL_DEFINITIONS = Object.freeze({
  globalMaxConcurrency: {
    label: 'Global Max Concurrency',
    tooltip: 'Maximum simultaneous API requests across all reports. Higher values fetch data faster but may overwhelm the server or browser.',
    min: 1,
    max: 20,
    step: 1,
    unit: 'requests',
    group: 'concurrency',
  },
  greenZoneEnabled: {
    label: 'Enable Green Zone',
    tooltip: 'When enabled, the system automatically boosts concurrency when all reports are responding quickly and memory is healthy.',
    type: 'toggle',
    group: 'greenZone',
  },
  greenZoneConcurrencyMax: {
    label: 'Green Zone Boost Level',
    tooltip: 'Maximum concurrency allowed when Green Zone is active. Only takes effect when Green Zone is enabled and conditions are favorable.',
    min: 8,
    max: 20,
    step: 1,
    unit: 'requests',
    group: 'greenZone',
  },
  greenZoneStreakCount: {
    label: 'Entry Streak Count',
    tooltip: 'Number of consecutive "good" latency samples required before entering Green Zone. Lower = more aggressive boosting, higher = more conservative.',
    min: 1,
    max: 10,
    step: 1,
    unit: 'samples',
    group: 'greenZone',
  },
  fetchBufferSize: {
    label: 'Fetch Buffer Size',
    tooltip: 'Maximum pages to download ahead of processing. Larger buffers keep workers busy but use more memory.',
    min: 2,
    max: 20,
    step: 1,
    unit: 'pages',
    group: 'pipeline',
  },
  processingPoolSize: {
    label: 'Processing Pool Size',
    tooltip: 'Number of pages to process simultaneously. Higher values increase CPU usage but can speed up analysis.',
    min: 1,
    max: 6,
    step: 1,
    unit: 'tasks',
    group: 'pipeline',
  },
  pageQueueLimit: {
    label: 'Page Queue Limit',
    tooltip: 'Maximum pages waiting in the prefetch queue. Acts as a memory safety valve.',
    min: 5,
    max: 30,
    step: 1,
    unit: 'pages',
    group: 'pipeline',
  },
  forceTier: {
    label: 'Force Tier',
    tooltip: 'Override automatic dataset size detection. Use "Auto" for normal operation, or force a specific tier to test behavior with different dataset sizes.',
    type: 'select',
    options: ['auto', 'tiny', 'small', 'medium', 'large', 'huge'],
    optionLabels: {
      auto: 'Auto (detect from data)',
      tiny: 'Tiny (<50 pages)',
      small: 'Small (<200 pages)',
      medium: 'Medium (<500 pages)',
      large: 'Large (<1000 pages)',
      huge: 'Huge (1000+ pages)',
    },
    group: 'pipeline',
  },
  batchSize: {
    label: 'Batch Size',
    tooltip: 'Number of rows sent to the Web Worker at once. Larger batches reduce transfer overhead but increase memory spikes.',
    min: 100,
    max: 2000,
    step: 50,
    unit: 'rows',
    group: 'worker',
  },
  partialUpdateInterval: {
    label: 'Partial Update Interval',
    tooltip: 'How often the UI receives progress updates during processing. Faster updates feel more responsive but add overhead.',
    min: 200,
    max: 5000,
    step: 100,
    unit: 'ms',
    group: 'worker',
  },
});

// ============================================================================
// RUNTIME STATE
// ============================================================================

let currentOverrides = { ...SYSTEM_DEFAULTS };
let changeListeners = [];

/**
 * Get the current effective configuration (defaults merged with overrides)
 */
export function getConfig() {
  return { ...currentOverrides };
}

/**
 * Get a specific configuration value
 */
export function getConfigValue(key) {
  return currentOverrides[key];
}

/**
 * Check if a specific setting differs from the system default
 */
export function isOverridden(key) {
  return currentOverrides[key] !== SYSTEM_DEFAULTS[key];
}

/**
 * Check if any settings differ from defaults
 */
export function hasAnyOverrides() {
  return Object.keys(SYSTEM_DEFAULTS).some(key => isOverridden(key));
}

/**
 * Update a single configuration value
 */
export function setConfigValue(key, value) {
  if (!(key in SYSTEM_DEFAULTS)) {
    console.warn(`Unknown backpressure config key: ${key}`);
    return;
  }

  const oldValue = currentOverrides[key];
  currentOverrides[key] = value;

  if (oldValue !== value) {
    notifyListeners(key, value, oldValue);
  }
}

/**
 * Apply a complete configuration (e.g., from a preset)
 */
export function applyConfig(config) {
  const changes = [];

  for (const key of Object.keys(SYSTEM_DEFAULTS)) {
    const newValue = config[key] ?? SYSTEM_DEFAULTS[key];
    const oldValue = currentOverrides[key];

    if (oldValue !== newValue) {
      currentOverrides[key] = newValue;
      changes.push({ key, oldValue, newValue });
    }
  }

  if (changes.length > 0) {
    notifyListeners('*', currentOverrides, changes);
  }
}

/**
 * Reset all settings to system defaults
 */
export function resetToDefaults() {
  applyConfig(SYSTEM_DEFAULTS);
}

/**
 * Apply a preset by name
 */
export function applyPreset(presetId) {
  const preset = PRESETS[presetId];
  if (!preset) {
    console.warn(`Unknown preset: ${presetId}`);
    return false;
  }
  applyConfig(preset.config);
  return true;
}

/**
 * Register a listener for configuration changes
 */
export function addChangeListener(callback) {
  changeListeners.push(callback);
  return () => {
    changeListeners = changeListeners.filter(cb => cb !== callback);
  };
}

function notifyListeners(key, newValue, oldValue) {
  for (const listener of changeListeners) {
    try {
      listener(key, newValue, oldValue);
    } catch (err) {
      console.error('Backpressure config listener error:', err);
    }
  }
}

/**
 * Get the effective tier configuration based on page count and forceTier setting
 */
export function getEffectiveTier(totalPages) {
  const forceTier = currentOverrides.forceTier;

  if (forceTier && forceTier !== 'auto' && TIER_CONFIGS[forceTier]) {
    return { tier: forceTier, config: TIER_CONFIGS[forceTier], forced: true };
  }

  // Auto-select based on page count
  const tiers = ['tiny', 'small', 'medium', 'large', 'huge'];
  for (const tierName of tiers) {
    const tierConfig = TIER_CONFIGS[tierName];
    if (totalPages <= tierConfig.maxPages) {
      return { tier: tierName, config: tierConfig, forced: false };
    }
  }

  return { tier: 'huge', config: TIER_CONFIGS.huge, forced: false };
}
