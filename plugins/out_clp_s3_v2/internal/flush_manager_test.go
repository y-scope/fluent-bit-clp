package internal

import (
	"sync"
	"testing"
	"time"
)

const (
	testDeltaDuration   = 50 * time.Millisecond
	testWaitGroupsDelta = 1
)

func TestFlushManager(t *testing.T) {
	flushConfig := &FlushConfigContext{
		defaultLogLevel: 0,
		hardDeltas:      []time.Duration{testDeltaDuration},
		softDeltas:      []time.Duration{testDeltaDuration},
	}

	var wg sync.WaitGroup
	wg.Add(testWaitGroupsDelta)

	flushCtx := &flushContext{
		HardTimer: time.NewTimer(0),
		SoftTimer: time.NewTimer(0),
		userCallback: func() {
			t.Logf("flush occurred")
			wg.Done()
		},
	}

	// Trigger an update that should schedule the flush
	flushCtx.Update(0, time.Now(), flushConfig)

	// Wait for the callback or timeout after 1s
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("flush callback was not called within 1 second")
	}
}

func TestGetDeltaSafe_ValidLevel(t *testing.T) {
	deltas := []time.Duration{
		100 * time.Millisecond, // level 0 (debug)
		200 * time.Millisecond, // level 1 (info)
		300 * time.Millisecond, // level 2 (warn)
		400 * time.Millisecond, // level 3 (error)
		500 * time.Millisecond, // level 4 (fatal)
	}
	defaultLevel := 1

	tests := []struct {
		name     string
		level    int
		expected time.Duration
	}{
		{"level 0", 0, 100 * time.Millisecond},
		{"level 1", 1, 200 * time.Millisecond},
		{"level 2", 2, 300 * time.Millisecond},
		{"level 3", 3, 400 * time.Millisecond},
		{"level 4", 4, 500 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDeltaSafe(tt.level, deltas, defaultLevel, "test")
			if result != tt.expected {
				t.Errorf("getDeltaSafe(%d) = %v, want %v", tt.level, result, tt.expected)
			}
		})
	}
}

func TestGetDeltaSafe_InvalidLevel_UsesDefault(t *testing.T) {
	deltas := []time.Duration{
		100 * time.Millisecond, // level 0
		200 * time.Millisecond, // level 1
		300 * time.Millisecond, // level 2
	}
	defaultLevel := 1

	tests := []struct {
		name     string
		level    int
		expected time.Duration
	}{
		{"negative level", -1, 200 * time.Millisecond},
		{"level out of range", 5, 200 * time.Millisecond},
		{"level way out of range", 100, 200 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDeltaSafe(tt.level, deltas, defaultLevel, "test")
			if result != tt.expected {
				t.Errorf("getDeltaSafe(%d) = %v, want %v (default)", tt.level, result, tt.expected)
			}
		})
	}
}

func TestGetDeltaSafe_InvalidDefaultLevel_FallsBackTo1Second(t *testing.T) {
	deltas := []time.Duration{
		100 * time.Millisecond, // level 0
	}
	invalidDefaultLevel := 10 // out of range

	result := getDeltaSafe(5, deltas, invalidDefaultLevel, "test")
	expected := time.Second // hardcoded fallback
	if result != expected {
		t.Errorf("getDeltaSafe with invalid default = %v, want %v", result, expected)
	}
}

func TestGetDeltaSafe_EmptyDeltas_FallsBackTo1Second(t *testing.T) {
	deltas := []time.Duration{} // empty
	defaultLevel := 0

	result := getDeltaSafe(0, deltas, defaultLevel, "test")
	expected := time.Second // hardcoded fallback
	if result != expected {
		t.Errorf("getDeltaSafe with empty deltas = %v, want %v", result, expected)
	}
}

func TestStopTimer_NilTimer(t *testing.T) {
	var timer *time.Timer = nil
	// Should not panic
	stopTimer(&timer)
	if timer != nil {
		t.Error("timer should remain nil")
	}
}

func TestStopTimer_ActiveTimer(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	stopTimer(&timer)
	if timer != nil {
		t.Error("timer should be nil after stopTimer")
	}
}

func TestReplaceTimer(t *testing.T) {
	callCount := 0
	callback := func() {
		callCount++
	}

	var timer *time.Timer = nil
	replaceTimer(&timer, 10*time.Millisecond, callback)

	if timer == nil {
		t.Fatal("timer should not be nil after replaceTimer")
	}

	// Wait for callback
	time.Sleep(50 * time.Millisecond)
	if callCount != 1 {
		t.Errorf("callback count = %d, want 1", callCount)
	}
}

func TestReplaceTimer_ReplacesExisting(t *testing.T) {
	firstCallCount := 0
	secondCallCount := 0

	firstCallback := func() {
		firstCallCount++
	}
	secondCallback := func() {
		secondCallCount++
	}

	var timer *time.Timer = nil

	// Set first timer with long duration
	replaceTimer(&timer, time.Hour, firstCallback)

	// Replace with second timer with short duration
	replaceTimer(&timer, 10*time.Millisecond, secondCallback)

	// Wait for second callback
	time.Sleep(50 * time.Millisecond)

	if firstCallCount != 0 {
		t.Errorf("first callback should not have been called, got %d", firstCallCount)
	}
	if secondCallCount != 1 {
		t.Errorf("second callback count = %d, want 1", secondCallCount)
	}
}

func TestFlushContext_MultipleUpdates_PreservesEarlierHardTimeout(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	flushCtx := &flushContext{
		HardTimer: time.NewTimer(0),
		SoftTimer: time.NewTimer(0),
		userCallback: func() {
			mu.Lock()
			callCount++
			mu.Unlock()
		},
	}

	// Stop initial timers to reset state
	flushCtx.HardTimer.Stop()
	flushCtx.SoftTimer.Stop()

	// Set an initial hard timeout
	now := time.Now()
	initialHardTimeout := now.Add(50 * time.Millisecond)
	flushCtx.hardTimeout = initialHardTimeout

	flushConfig := &FlushConfigContext{
		defaultLogLevel: 0,
		hardDeltas:      []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
		softDeltas:      []time.Duration{time.Hour, time.Hour}, // soft won't fire
	}

	// Update with level 0 (100ms hard delta) - should NOT change hardTimeout because
	// now.Add(100ms) is AFTER the existing hardTimeout of now.Add(50ms)
	flushCtx.Update(0, now, flushConfig)

	if !flushCtx.hardTimeout.Equal(initialHardTimeout) {
		t.Errorf("hard timeout should be preserved when new timeout is later: got=%v, want=%v",
			flushCtx.hardTimeout, initialHardTimeout)
	}
}

func TestFlushContext_Update_SetsEarlierHardTimeout(t *testing.T) {
	flushCtx := &flushContext{
		HardTimer: time.NewTimer(0),
		SoftTimer: time.NewTimer(0),
		userCallback: func() {
			// no-op
		},
	}

	// Stop initial timers
	flushCtx.HardTimer.Stop()
	flushCtx.SoftTimer.Stop()

	// Set an initial hard timeout that's far in the future
	now := time.Now()
	initialHardTimeout := now.Add(time.Hour)
	flushCtx.hardTimeout = initialHardTimeout

	flushConfig := &FlushConfigContext{
		defaultLogLevel: 0,
		hardDeltas:      []time.Duration{50 * time.Millisecond},
		softDeltas:      []time.Duration{time.Hour},
	}

	// Update with level 0 (50ms hard delta) - should change hardTimeout because
	// now.Add(50ms) is BEFORE the existing hardTimeout of now.Add(1hr)
	flushCtx.Update(0, now, flushConfig)

	if flushCtx.hardTimeout.Equal(initialHardTimeout) {
		t.Error("hard timeout should have been updated to earlier time")
	}
	if !flushCtx.hardTimeout.Before(initialHardTimeout) {
		t.Errorf("new hard timeout should be before initial: got=%v, initial=%v",
			flushCtx.hardTimeout, initialHardTimeout)
	}
}

func TestFlushContext_Callback_ResetsState(t *testing.T) {
	callCount := 0

	flushCtx := &flushContext{
		HardTimer: time.NewTimer(time.Hour),
		SoftTimer: time.NewTimer(time.Hour),
		userCallback: func() {
			callCount++
		},
		hardTimeout: time.Now().Add(time.Hour),
		softDelta:   time.Minute,
	}

	flushCtx.Callback()

	if !flushCtx.hardTimeout.IsZero() {
		t.Error("hardTimeout should be zero after Callback")
	}
	if flushCtx.HardTimer != nil {
		t.Error("HardTimer should be nil after Callback")
	}
	if flushCtx.SoftTimer != nil {
		t.Error("SoftTimer should be nil after Callback")
	}
	if callCount != 1 {
		t.Errorf("userCallback should have been called once, got %d", callCount)
	}
}
