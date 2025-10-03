package rindb

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeTicker struct {
	c       chan time.Time
	mu      sync.Mutex
	stopped bool
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{c: make(chan time.Time, 1)}
}

func (f *fakeTicker) C() <-chan time.Time {
	return f.c
}

func (f *fakeTicker) Stop() {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
}

func (f *fakeTicker) Stopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

func TestIOLoadMonitor_RecordWriteEWMA(t *testing.T) {
	t.Parallel()

	current := time.Unix(0, 0)
	monitor := newIOLoadMonitor(func() time.Time { return current }, nil)

	monitor.RecordWrite()
	current = current.Add(500 * time.Millisecond)
	monitor.RecordWrite()
	current = current.Add(600 * time.Millisecond)
	monitor.RecordWrite()

	monitor.handleTick()

	rate, _ := monitor.CurrentMetrics()
	expected := writeRateAlpha * (float64(3) / 1.1)
	require.InDelta(t, expected, rate, 1e-9)
}

func TestIOLoadMonitor_SampleIOLoad(t *testing.T) {
	t.Parallel()

	current := time.Unix(0, 0)
	samples := make(chan uint64, 2)
	calls := make(chan struct{}, 2)
	monitor := newIOLoadMonitor(
		func() time.Time { return current },
		func() (uint64, error) {
			v := <-samples
			calls <- struct{}{}
			return v, nil
		},
	)

	samples <- 100
	monitor.sampleIOLoad(current)

	samples <- 250
	current = current.Add(150 * time.Millisecond)
	monitor.sampleIOLoad(current)

	_, load := monitor.CurrentMetrics()
	require.InDelta(t, 1.0, load, 1e-9)
}

func TestIOLoadMonitor_StartStop(t *testing.T) {
	t.Parallel()

	current := time.Unix(0, 0)
	samples := make(chan uint64, 2)
	calls := make(chan struct{}, 2)
	monitor := newIOLoadMonitor(
		func() time.Time { return current },
		func() (uint64, error) {
			v := <-samples
			calls <- struct{}{}
			return v, nil
		},
	)

	fake := newFakeTicker()
	monitor.newTicker = func(time.Duration) ticker { return fake }

	samples <- 0
	samples <- 200

	monitor.Start()

	fake.c <- time.Time{}
	require.Eventually(t, func() bool { return len(calls) >= 1 }, time.Second, 10*time.Millisecond)
	current = current.Add(200 * time.Millisecond)
	fake.c <- time.Time{}
	require.Eventually(t, func() bool { return len(calls) >= 2 }, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, load := monitor.CurrentMetrics()
		return load > 0.9
	}, time.Second, 10*time.Millisecond)

	monitor.Stop()
	require.True(t, fake.Stopped())

	// Stop should be idempotent.
	monitor.Stop()
}

func TestIOLoadMonitor_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var currentMu sync.Mutex
	current := time.Unix(0, 0)
	monitor := newIOLoadMonitor(func() time.Time {
		currentMu.Lock()
		defer currentMu.Unlock()
		return current
	}, nil)

	fake := newFakeTicker()
	monitor.newTicker = func(time.Duration) ticker { return fake }

	monitor.Start()

	const writers = 8
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				monitor.RecordWrite()
			}
		}()
	}

	for i := 0; i < 20; i++ {
		currentMu.Lock()
		current = current.Add(100 * time.Millisecond)
		currentMu.Unlock()
		fake.c <- time.Time{}
	}

	wg.Wait()
	monitor.Stop()

	monitor.CurrentMetrics()
}
