package utilities

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type MetricType string

const (
	MetricTypeQuery          MetricType = "query"
	MetricTypeMutation       MetricType = "mutation"
	MetricTypeDatabase       MetricType = "database"
	MetricTypeCache          MetricType = "cache"
	MetricTypeRouting        MetricType = "routing"
	MetricTypeAuthentication MetricType = "authentication"
)

type Metric struct {
	ID       string        // The id of the metric
	Name     string        // The name of the metric
	Type     MetricType    // The type of the metric
	Time     time.Time     // The time the metric was recorded
	Duration time.Duration // The duration of the metric
	Tags     []string      // The tracking tags of the metric
}

var ErrProfilerRunning = errors.New("tether: profiler is already running")

type Profiler struct {
	active   atomic.Bool
	mu       sync.Mutex
	metrics  []Metric
	capacity int
	stopChan chan struct{}
	onFlush  func(mutationName string)
}

func NewProfiler(onFlush func(mutationName string)) *Profiler {
	return &Profiler{
		metrics:  make([]Metric, 0, 2048),
		capacity: 100_000, // Safe ceiling to prevent runaway RAM usage
		onFlush:  onFlush,
	}
}

func (p *Profiler) IsActive() bool {
	return p.active.Load()
}

func (p *Profiler) Start() error {
	if !p.active.CompareAndSwap(false, true) {
		return ErrProfilerRunning
	}
	return nil
}

func (p *Profiler) StartWithCallback(flushInterval time.Duration, mutationName string) error {
	if !p.active.CompareAndSwap(false, true) {
		return ErrProfilerRunning
	}

	p.mu.Lock()
	p.stopChan = make(chan struct{})
	stop := p.stopChan
	p.mu.Unlock()

	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if p.onFlush != nil {
					p.onFlush(mutationName)
				}
			}
		}
	}()

	return nil
}

func (p *Profiler) Add(m Metric) {
	// Ultra-fast path: lock-free when inactive
	if !p.active.Load() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Guard against runaway memory if flushes are delayed
	if len(p.metrics) < p.capacity {
		p.metrics = append(p.metrics, m)
	}
}

func (p *Profiler) DumpMetricsAndFlush() []Metric {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.metrics) == 0 {
		return nil
	}

	flushed := p.metrics
	p.metrics = make([]Metric, 0, 2048)
	return flushed
}

func (p *Profiler) Stop() {
	if !p.active.CompareAndSwap(true, false) {
		return
	}

	p.mu.Lock()
	if p.stopChan != nil {
		close(p.stopChan)
		p.stopChan = nil
	}
	p.mu.Unlock()
}
