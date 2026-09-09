package utilities

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	Value    int           // The value of the metric
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

// ProfileOp is a single recorded span inside an execution.
type ProfileOp struct {
	Name     string        `json:"name"`
	Type     MetricType    `json:"type"`
	Time     time.Time     `json:"time"`
	Duration time.Duration `json:"duration"`
	Value    int           `json:"value,omitempty"`
	Tags     []string      `json:"tags,omitempty"`
}

// ProfiledExecution is one execution ID with nested work rolled up into
// non-overlapping components. Logic is the parent query/mutation wall time
// minus nested database (and cache/routing/auth) time.
type ProfiledExecution struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Type      MetricType    `json:"type"`
	StartedAt time.Time     `json:"started_at"`
	Tags      []string      `json:"tags,omitempty"`

	Total    time.Duration `json:"total"`
	Logic    time.Duration `json:"logic"`
	Database time.Duration `json:"database"`
	Cache    time.Duration `json:"cache"`
	Routing  time.Duration `json:"routing"`
	Auth     time.Duration `json:"auth"`

	DatabaseOps []ProfileOp `json:"database_ops,omitempty"`
	CacheOps    []ProfileOp `json:"cache_ops,omitempty"`
	RoutingOps  []ProfileOp `json:"routing_ops,omitempty"`
	AuthOps     []ProfileOp `json:"auth_ops,omitempty"`
	OtherOps    []ProfileOp `json:"other_ops,omitempty"`
}

// ProfileAggregate rolls up every execution that shares a sanitized name.
type ProfileAggregate struct {
	Name        string        `json:"name"`
	Type        MetricType    `json:"type"`
	Count       int           `json:"count"`
	TotalSum    time.Duration `json:"total_sum"`
	LogicSum    time.Duration `json:"logic_sum"`
	DatabaseSum time.Duration `json:"database_sum"`
	CacheSum    time.Duration `json:"cache_sum"`
	RoutingSum  time.Duration `json:"routing_sum"`
	AuthSum     time.Duration `json:"auth_sum"`
	TotalAvg    time.Duration `json:"total_avg"`
	LogicAvg    time.Duration `json:"logic_avg"`
	DatabaseAvg time.Duration `json:"database_avg"`
	RoutingAvg  time.Duration `json:"routing_avg"`
	TotalMin    time.Duration `json:"total_min"`
	TotalMax    time.Duration `json:"total_max"`
}

// ProfileTotals is the report-wide sum of each component.
type ProfileTotals struct {
	Executions int           `json:"executions"`
	Total      time.Duration `json:"total"`
	Logic      time.Duration `json:"logic"`
	Database   time.Duration `json:"database"`
	Cache      time.Duration `json:"cache"`
	Routing    time.Duration `json:"routing"`
	Auth       time.Duration `json:"auth"`
}

// ProfileReport is a sanitized, execution-batched view of raw profiler metrics.
type ProfileReport struct {
	Totals     ProfileTotals       `json:"totals"`
	Aggregates []ProfileAggregate  `json:"aggregates"`
	Executions []ProfiledExecution `json:"executions"`
}

const unattributedExecutionID = "unattributed"

// SanitizeMetrics groups raw metrics by execution ID and rewrites query and
// mutation wall clocks into component times. Database spans are summed;
// overlapping routing spans (cumulative batch_execution vs invalidate_tags)
// are collapsed so Logic is max(0, Total - Database - Cache - Routing - Auth).
func SanitizeMetrics(metrics []Metric) ProfileReport {
	if len(metrics) == 0 {
		return ProfileReport{}
	}

	order := make([]string, 0)
	grouped := make(map[string][]Metric, 16)
	for _, m := range metrics {
		id := m.ID
		if id == "" {
			id = unattributedExecutionID
		}
		if _, seen := grouped[id]; !seen {
			order = append(order, id)
		}
		grouped[id] = append(grouped[id], m)
	}

	executions := make([]ProfiledExecution, 0, len(order))
	for _, id := range order {
		executions = append(executions, sanitizeExecution(id, grouped[id]))
	}
	slices.SortFunc(executions, func(a, b ProfiledExecution) int {
		if c := a.StartedAt.Compare(b.StartedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	aggregates := aggregateExecutions(executions)
	totals := ProfileTotals{Executions: len(executions)}
	for _, exec := range executions {
		totals.Total += exec.Total
		totals.Logic += exec.Logic
		totals.Database += exec.Database
		totals.Cache += exec.Cache
		totals.Routing += exec.Routing
		totals.Auth += exec.Auth
	}

	return ProfileReport{
		Totals:     totals,
		Aggregates: aggregates,
		Executions: executions,
	}
}

// Slowest returns the n executions with the largest Total, longest first.
func (r ProfileReport) Slowest(n int) []ProfiledExecution {
	if n <= 0 || len(r.Executions) == 0 {
		return nil
	}
	ranked := slices.Clone(r.Executions)
	slices.SortFunc(ranked, func(a, b ProfiledExecution) int {
		if c := cmp.Compare(b.Total, a.Total); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	if n > len(ranked) {
		n = len(ranked)
	}
	return ranked[:n]
}

func (r ProfileReport) String() string {
	if len(r.Executions) == 0 {
		return "Profiler report (empty)"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Profiler report (%d executions)\n", r.Totals.Executions)
	fmt.Fprintf(&b, "  total=%s  logic=%s  database=%s  routing=%s  cache=%s  auth=%s\n",
		r.Totals.Total, r.Totals.Logic, r.Totals.Database, r.Totals.Routing, r.Totals.Cache, r.Totals.Auth)

	if len(r.Aggregates) > 0 {
		b.WriteString("\n  By name:\n")
		nameWidth := 0
		for _, agg := range r.Aggregates {
			if len(agg.Name) > nameWidth {
				nameWidth = len(agg.Name)
			}
		}
		for _, agg := range r.Aggregates {
			fmt.Fprintf(&b, "    %-*s  n=%-5d  total=%s (avg %s, max %s)  logic=%s  db=%s  routing=%s\n",
				nameWidth, agg.Name, agg.Count, agg.TotalSum, agg.TotalAvg, agg.TotalMax, agg.LogicAvg, agg.DatabaseAvg, agg.RoutingAvg)
		}
	}

	slowest := r.Slowest(15)
	if len(slowest) == 0 {
		return b.String()
	}
	b.WriteString("\n  Slowest executions:\n")
	for _, exec := range slowest {
		fmt.Fprintf(&b, "    %s  %s  total=%s  logic=%s  db=%s  routing=%s\n",
			exec.Type, exec.Name, exec.Total, exec.Logic, exec.Database, exec.Routing)
		writeOps(&b, "database", exec.DatabaseOps)
		writeOps(&b, "cache", exec.CacheOps)
		writeOps(&b, "routing", exec.RoutingOps)
		writeOps(&b, "auth", exec.AuthOps)
		writeOps(&b, "other", exec.OtherOps)
	}
	return b.String()
}

func writeOps(b *strings.Builder, kind string, ops []ProfileOp) {
	for _, op := range ops {
		fmt.Fprintf(b, "      %-9s  %s  %s", kind, op.Name, op.Duration)
		if op.Value != 0 {
			fmt.Fprintf(b, "  value=%d", op.Value)
		}
		if len(op.Tags) > 0 {
			fmt.Fprintf(b, "  tags=%s", strings.Join(op.Tags, ","))
		}
		b.WriteByte('\n')
	}
}

func sanitizeExecution(id string, metrics []Metric) ProfiledExecution {
	primaryIdx := pickPrimaryMetricIndex(metrics)
	primary := metrics[primaryIdx]
	hasEnvelope := primaryRank(primary.Type) >= primaryRank(MetricTypeRouting)

	exec := ProfiledExecution{
		ID:   id,
		Type: primary.Type,
	}
	if hasEnvelope {
		exec.Name = sanitizeMetricName(primary)
		exec.Total = primary.Duration
	} else if id == unattributedExecutionID {
		exec.Name = unattributedExecutionID
	} else {
		exec.Name = string(primary.Type)
	}
	if !primary.Time.IsZero() {
		exec.StartedAt = primary.Time
	}

	for i := range metrics {
		m := metrics[i]
		if !m.Time.IsZero() && (exec.StartedAt.IsZero() || m.Time.Before(exec.StartedAt)) {
			exec.StartedAt = m.Time
		}
		if hasEnvelope && i == primaryIdx {
			exec.Tags = mergeTags(exec.Tags, m.Tags)
			continue
		}

		op := ProfileOp{
			Name:     sanitizeMetricName(m),
			Type:     m.Type,
			Time:     m.Time,
			Duration: m.Duration,
			Value:    m.Value,
			Tags:     cloneTags(m.Tags),
		}
		switch m.Type {
		case MetricTypeDatabase:
			exec.DatabaseOps = append(exec.DatabaseOps, op)
			exec.Database += m.Duration
		case MetricTypeCache:
			exec.CacheOps = append(exec.CacheOps, op)
			exec.Cache += m.Duration
		case MetricTypeRouting:
			exec.RoutingOps = append(exec.RoutingOps, op)
		case MetricTypeAuthentication:
			exec.AuthOps = append(exec.AuthOps, op)
			exec.Auth += m.Duration
		default:
			exec.OtherOps = append(exec.OtherOps, op)
		}
		exec.Tags = mergeTags(exec.Tags, m.Tags)
	}

	exec.Routing = routingDuration(exec.RoutingOps)
	sortOps(exec.DatabaseOps)
	sortOps(exec.CacheOps)
	sortOps(exec.RoutingOps)
	sortOps(exec.AuthOps)
	sortOps(exec.OtherOps)

	nested := exec.Database + exec.Cache + exec.Routing + exec.Auth
	if exec.Total > nested {
		exec.Logic = exec.Total - nested
	}
	if exec.Total == 0 {
		exec.Total = nested
	}
	return exec
}

func pickPrimaryMetricIndex(metrics []Metric) int {
	best := 0
	bestRank := primaryRank(metrics[0].Type)
	for i := 1; i < len(metrics); i++ {
		rank := primaryRank(metrics[i].Type)
		if rank > bestRank || (rank == bestRank && metrics[i].Duration > metrics[best].Duration) {
			best = i
			bestRank = rank
		}
	}
	return best
}

func primaryRank(t MetricType) int {
	switch t {
	case MetricTypeMutation:
		return 5
	case MetricTypeQuery:
		return 4
	case MetricTypeAuthentication:
		return 3
	case MetricTypeRouting:
		return 2
	default:
		return 1
	}
}

func sanitizeMetricName(m Metric) string {
	if m.Type == MetricTypeAuthentication {
		return string(MetricTypeAuthentication)
	}
	if m.Name == "" {
		return string(m.Type)
	}
	return m.Name
}

func routingDuration(ops []ProfileOp) time.Duration {
	var tagged, max time.Duration
	for _, op := range ops {
		if op.Duration > max {
			max = op.Duration
		}
		if strings.HasPrefix(op.Name, "invalidate_tags:") {
			tagged += op.Duration
		}
	}
	if tagged > 0 {
		return tagged
	}
	return max
}

func aggregateExecutions(executions []ProfiledExecution) []ProfileAggregate {
	index := make(map[string]int, len(executions))
	aggregates := make([]ProfileAggregate, 0)
	for _, exec := range executions {
		key := string(exec.Type) + "\x00" + exec.Name
		i, ok := index[key]
		if !ok {
			index[key] = len(aggregates)
			aggregates = append(aggregates, ProfileAggregate{
				Name:     exec.Name,
				Type:     exec.Type,
				TotalMin: exec.Total,
				TotalMax: exec.Total,
			})
			i = len(aggregates) - 1
		}
		agg := &aggregates[i]
		agg.Count++
		agg.TotalSum += exec.Total
		agg.LogicSum += exec.Logic
		agg.DatabaseSum += exec.Database
		agg.CacheSum += exec.Cache
		agg.RoutingSum += exec.Routing
		agg.AuthSum += exec.Auth
		if exec.Total < agg.TotalMin {
			agg.TotalMin = exec.Total
		}
		if exec.Total > agg.TotalMax {
			agg.TotalMax = exec.Total
		}
	}
	for i := range aggregates {
		n := time.Duration(aggregates[i].Count)
		if n == 0 {
			continue
		}
		aggregates[i].TotalAvg = aggregates[i].TotalSum / n
		aggregates[i].LogicAvg = aggregates[i].LogicSum / n
		aggregates[i].DatabaseAvg = aggregates[i].DatabaseSum / n
		aggregates[i].RoutingAvg = aggregates[i].RoutingSum / n
	}
	slices.SortFunc(aggregates, func(a, b ProfileAggregate) int {
		if c := cmp.Compare(b.TotalSum, a.TotalSum); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return aggregates
}

func sortOps(ops []ProfileOp) {
	slices.SortFunc(ops, func(a, b ProfileOp) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
}

func cloneTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	out := make([]string, len(tags))
	copy(out, tags)
	return out
}

func mergeTags(existing []string, incoming []string) []string {
	if len(incoming) == 0 {
		return existing
	}
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	for _, tag := range existing {
		seen[tag] = struct{}{}
	}
	out := existing
	for _, tag := range incoming {
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}
