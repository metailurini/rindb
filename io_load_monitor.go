package rindb

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	*time.Ticker
}

// writeRateAlpha is the smoothing factor for write-rate exponential moving
// average. A higher value weights recent samples more heavily.
const writeRateAlpha = 0.2

const unsetWriteSample int64 = math.MinInt64

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}

func (t realTicker) Stop() {
	t.Ticker.Stop()
}

// ioLoadMonitor tracks write throughput and disk utilisation for SSTable compaction decisions.
type ioLoadMonitor struct {
	writeCounter    atomic.Uint64
	lastWriteSample atomic.Int64
	writeRateBits   atomic.Uint64
	ioLoadBits      atomic.Uint64
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
	m.lastWriteSample.Store(unsetWriteSample)
	m.writeRateBits.Store(math.Float64bits(0))
	m.ioLoadBits.Store(math.Float64bits(0))
	return m
}

// RecordWrite records a write and updates the EWMA write rate once the sampling interval elapses.
func (m *ioLoadMonitor) RecordWrite() {
	m.writeCounter.Add(1)
	now := m.now().UnixNano()
	if m.lastWriteSample.Load() == unsetWriteSample {
		m.lastWriteSample.CompareAndSwap(unsetWriteSample, now)
	}
}

// sampleIOLoad calculates the fraction of time the disk was busy since the previous sample.
func (m *ioLoadMonitor) sampleIOLoad(now time.Time) {
	total, err := m.diskSampler()
	if err != nil {
		return
	}

	if !m.lastIOSample.IsZero() {
		if total < m.lastIOTotal {
			m.lastIOTotal = total
			m.lastIOSample = now
			return
		}
		deltaIO := total - m.lastIOTotal
		deltaTime := now.Sub(m.lastIOSample).Milliseconds()
		if deltaTime > 0 {
			load := float64(deltaIO) / float64(deltaTime)
			m.ioLoadBits.Store(math.Float64bits(load))
		}
	}

	m.lastIOTotal = total
	m.lastIOSample = now
}

func (m *ioLoadMonitor) updateWriteRate(now time.Time) {
	writes := m.writeCounter.Swap(0)
	lastSample := m.lastWriteSample.Load()
	if lastSample == unsetWriteSample {
		if writes > 0 {
			m.writeCounter.Add(writes)
		}
		return
	}

	elapsed := now.Sub(time.Unix(0, lastSample)).Seconds()
	if elapsed <= 0 {
		if writes > 0 {
			m.writeCounter.Add(writes)
		}
		return
	}

	rate := float64(writes) / elapsed
	prev := math.Float64frombits(m.writeRateBits.Load())
	ewma := (1-writeRateAlpha)*prev + writeRateAlpha*rate
	m.writeRateBits.Store(math.Float64bits(ewma))
	m.lastWriteSample.Store(now.UnixNano())
}

func (m *ioLoadMonitor) handleTick() {
	now := m.now()
	m.updateWriteRate(now)
	m.sampleIOLoad(now)
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
					m.handleTick()
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
	writeRate := math.Float64frombits(m.writeRateBits.Load())
	ioLoad := math.Float64frombits(m.ioLoadBits.Load())
	return writeRate, ioLoad
}
