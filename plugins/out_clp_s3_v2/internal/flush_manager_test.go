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
