package rindb

import (
	"sync"
	"time"
)

type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}

func (t realTicker) Stop() {
	t.Ticker.Stop()
}

// ioLoadMonitor tracks write throughput and disk utilisation for SSTable compaction decisions.
type ioLoadMonitor struct {
	mu sync.Mutex

	writeCounter    uint64
	writeRate       float64
	ioLoad          float64
	lastWriteSample time.Time
	lastIOTotal     uint64
	lastIOSample    time.Time

	now         func() time.Time
	diskSampler func() (uint64, error)
	newTicker   func(time.Duration) ticker

	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

func newIOLoadMonitor(now func() time.Time, diskSampler func() (uint64, error)) *ioLoadMonitor {
	m := &ioLoadMonitor{
		now:         now,
		diskSampler: diskSampler,
		stopCh:      make(chan struct{}),
	}
	if m.now == nil {
		m.now = time.Now
	}
	if m.diskSampler == nil {
		m.diskSampler = func() (uint64, error) { return 0, nil }
	}
	m.newTicker = func(d time.Duration) ticker { return realTicker{time.NewTicker(d)} }
	return m
}

// RecordWrite records a write and updates the EWMA write rate once the sampling interval elapses.
func (m *ioLoadMonitor) RecordWrite() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeCounter++
	now := m.now()
	if m.lastWriteSample.IsZero() {
		m.lastWriteSample = now
		return
	}
	if now.Sub(m.lastWriteSample) >= time.Second {
		duration := now.Sub(m.lastWriteSample).Seconds()
		rate := float64(m.writeCounter) / duration
		m.writeRate = (1-writeRateAlpha)*m.writeRate + writeRateAlpha*rate
		m.writeCounter = 0
		m.lastWriteSample = now
	}
}

// sampleIOLoad calculates the fraction of time the disk was busy since the previous sample.
func (m *ioLoadMonitor) sampleIOLoad() {
	total, err := m.diskSampler()
	if err != nil {
		return
	}

	now := m.now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.lastIOSample.IsZero() {
		if total < m.lastIOTotal {
			m.lastIOTotal = total
			m.lastIOSample = now
			return
		}
		deltaIO := total - m.lastIOTotal
		deltaTime := now.Sub(m.lastIOSample).Milliseconds()
		if deltaTime > 0 {
			m.ioLoad = float64(deltaIO) / float64(deltaTime)
		}
	}

	m.lastIOTotal = total
	m.lastIOSample = now
}

// Start launches the periodic disk utilisation sampler.
func (m *ioLoadMonitor) Start() {
	m.startOnce.Do(func() {
		ticker := m.newTicker(time.Second)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer ticker.Stop()
			for {
				select {
				case <-m.stopCh:
					return
				case <-ticker.C():
					m.sampleIOLoad()
				}
			}
		}()
	})
}

// Stop terminates the sampling goroutine.
func (m *ioLoadMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.wg.Wait()
	})
}

// CurrentMetrics returns the latest write rate and disk utilisation measurements.
func (m *ioLoadMonitor) CurrentMetrics() (float64, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeRate, m.ioLoad
}
