package internal

import (
	"log"
	"math"
	"time"
)

/*
Dual-Timer Flush Strategy

This file implements a dual-timer flush strategy that balances log freshness against
upload costs. The strategy uses two independent timers per log stream:

1. HARD TIMER (Deadline Guarantee)
   - Sets an absolute deadline: logs MUST be uploaded by this time
   - Once set, only moves EARLIER (never later) when higher-severity logs arrive
   - Prevents logs from sitting too long without upload
   - Example: If hard delta is 30s, logs are guaranteed to upload within 30s

2. SOFT TIMER (Inactivity Detection)
   - Resets on EVERY log event
   - Fires after a period of no new logs (inactivity)
   - Triggers upload during quiet periods, even before hard deadline
   - Example: If soft delta is 5s, upload happens 5s after last log

Why Two Timers?

Consider a stream receiving logs sporadically:
  - Soft timer alone: Would never fire during continuous logging
  - Hard timer alone: Would always wait the full duration

Together they provide:
  - Fast upload during quiet periods (soft timer)
  - Guaranteed maximum latency (hard timer)
  - Upload storms prevention (timers prevent simultaneous uploads)

Log Level Awareness

Higher severity logs (ERROR, FATAL) can have shorter deltas, meaning:
  - An ERROR log triggers faster sync of the ENTIRE log file
  - This ensures critical logs reach S3 quickly for investigation
  - Lower severity logs (DEBUG, INFO) can have longer deltas to reduce costs
*/

// FlushManager defines the interface for updating flush timing based on log events.
type FlushManager interface {
	// Update recalculates flush timers based on a new log event's level and timestamp.
	Update(level int, timestamp time.Time, flushConfig *FlushConfigContext)
}

// Callback is invoked when either the hard or soft timer fires.
//
// This method:
//  1. Acquires the mutex to prevent concurrent timer modifications
//  2. Stops and clears both timers (prevents double-firing)
//  3. Resets state for the next batch of logs
//  4. Invokes the user callback (S3 upload)
//
// After Callback completes, the flushContext is ready for new log events.
func (m *flushContext) Callback() {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Stop both timers to prevent double-firing
	m.stopAndClearTimers()

	// Reset state for next batch
	m.hardTimeout = time.Time{}
	m.softDelta = time.Duration(math.MaxInt64)

	// Trigger the upload
	m.userCallback()
}

// Update adjusts the hard and soft timers based on a new log event.
//
// Hard timer behavior:
//   - If no hard timeout is set (first event), schedule based on log level
//   - If new event would trigger EARLIER upload, update the timer
//   - Never extends the hard deadline (only shortens it)
//
// Soft timer behavior:
//   - Always resets on each event (detects inactivity)
//   - Tracks the minimum soft delta seen (for mixed-level batches)
//
// Parameters:
//   - level: Log severity level (0=debug, 1=info, 2=warn, 3=error, 4=fatal)
//   - timestamp: When the log event occurred
//   - flushConfig: Configuration containing timer durations per level
func (m *flushContext) Update(level int, timestamp time.Time, flushConfig *FlushConfigContext) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Calculate and potentially update hard timer
	hardDelta := getDeltaSafe(level, flushConfig.hardDeltas, flushConfig.defaultLogLevel, "hard")
	nextHardTimeout := timestamp.Add(hardDelta)

	// Update hard timer only if:
	// 1. No hard timeout is set yet (zero value), OR
	// 2. New timeout is earlier than current (higher severity log arrived)
	if m.hardTimeout.IsZero() || nextHardTimeout.Before(m.hardTimeout) {
		replaceTimer(&m.HardTimer, time.Until(nextHardTimeout), m.Callback)
		m.hardTimeout = nextHardTimeout
	}

	// Track minimum soft delta (for mixed-level log batches)
	softDelta := getDeltaSafe(level, flushConfig.softDeltas, flushConfig.defaultLogLevel, "soft")
	if softDelta < m.softDelta {
		m.softDelta = softDelta
	}

	// Always reset soft timer on each event (inactivity detection)
	nextSoftTimeout := timestamp.Add(m.softDelta)
	replaceTimer(&m.SoftTimer, time.Until(nextSoftTimeout), m.Callback)
}

// getDeltaSafe returns the flush delta for a log level, with fallback handling.
//
// If the level is out of range (negative or >= len(deltas)), falls back to:
//  1. The default level's delta (if default is in range)
//  2. 1 second (hardcoded fallback if default is also invalid)
//
// This defensive approach prevents panics from unexpected log levels while
// logging a warning for investigation.
func getDeltaSafe(level int, deltas []time.Duration, defaultLevel int, label string) time.Duration {
	// Happy path: level is valid
	if level >= 0 && level < len(deltas) {
		return deltas[level]
	}

	// Fallback: use default level's delta
	defaultDelta := time.Second // Ultimate fallback
	if defaultLevel >= 0 && defaultLevel < len(deltas) {
		defaultDelta = deltas[defaultLevel]
	}

	log.Printf(
		"[warn] No %s flush delta found for log level %v; defaulting to level %v (%v).",
		label, level, defaultLevel, defaultDelta,
	)
	return defaultDelta
}

// stopAndClearTimers stops both hard and soft timers and sets them to nil.
// Safe to call even if timers are already nil.
func (m *flushContext) stopAndClearTimers() {
	stopTimer(&m.HardTimer)
	stopTimer(&m.SoftTimer)
}

// stopTimer safely stops a timer if it is not nil, then sets it to nil.
// The double-pointer allows modifying the caller's timer variable.
func stopTimer(timer **time.Timer) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
}

// replaceTimer stops the existing timer (if any) and creates a new one.
//
// Parameters:
//   - timer: Pointer to the timer variable to replace
//   - duration: How long until the timer fires
//   - callback: Function to call when timer fires
func replaceTimer(timer **time.Timer, duration time.Duration, callback func()) {
	stopTimer(timer)
	*timer = time.AfterFunc(duration, callback)
}
