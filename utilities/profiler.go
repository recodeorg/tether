package utilities

import (
	"sync"
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
	ID       string            // The id of the metric
	Name     string            // The name of the metric
	Type     MetricType        // The type of the metric
	Time     time.Time         // The time the metric was recorded
	Duration time.Duration     // The duration of the metric
	Tags     map[string]string // The tracking tags of the metric
}

type Profiler struct {
	mu             sync.Mutex
	profilerActive bool
	metrics        []Metric
	onFlush        func(mutationName string)
}

func NewProfiler(onFlush func(mutationName string)) *Profiler {
	return &Profiler{
		metrics: make([]Metric, 0),
		onFlush: onFlush,
	}
}

func (p *Profiler) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.profilerActive = true
}

func (p *Profiler) StartWithCallback(flushInterval time.Duration, mutationName string) {
	p.mu.Lock()
	if p.profilerActive {
		p.mu.Unlock()
		return
	}
	p.profilerActive = true
	p.mu.Unlock()
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for range ticker.C {
			p.mu.Lock()
			isActive := p.profilerActive
			p.mu.Unlock()
			if !isActive {
				return
			}
			p.onFlush(mutationName)
		}
	}()
}

func (p *Profiler) Add(m Metric) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.profilerActive {
		return
	}
	p.metrics = append(p.metrics, m)
}

func (p *Profiler) DumpMetricsAndFlush() []Metric {
	p.mu.Lock()
	defer p.mu.Unlock()
	metrics := p.metrics
	p.metrics = make([]Metric, 0, len(metrics))
	return metrics
}

func (p *Profiler) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.profilerActive = false
}
