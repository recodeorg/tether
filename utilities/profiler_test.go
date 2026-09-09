package utilities

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeMetricsEmpty(t *testing.T) {
	report := SanitizeMetrics(nil)
	if len(report.Executions) != 0 || report.String() != "Profiler report (empty)" {
		t.Fatalf("empty metrics: %+v", report)
	}
	report = SanitizeMetrics([]Metric{})
	if len(report.Executions) != 0 {
		t.Fatalf("empty slice should produce no executions, got %d", len(report.Executions))
	}
}

func TestSanitizeMetricsQuerySubtractsDB(t *testing.T) {
	start := time.Now()
	report := SanitizeMetrics([]Metric{
		{
			ID:       "q1",
			Name:     "gorm:query",
			Type:     MetricTypeDatabase,
			Time:     start,
			Duration: 7 * time.Millisecond,
			Tags:     []string{"messages_room_id:alpha"},
		},
		{
			ID:       "q1",
			Name:     "query:getMessages",
			Type:     MetricTypeQuery,
			Time:     start,
			Duration: 10 * time.Millisecond,
		},
	})
	if len(report.Executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(report.Executions))
	}
	exec := report.Executions[0]
	if exec.Name != "query:getMessages" || exec.Type != MetricTypeQuery {
		t.Fatalf("primary = %s %s", exec.Type, exec.Name)
	}
	if exec.Total != 10*time.Millisecond {
		t.Fatalf("total = %s, want 10ms", exec.Total)
	}
	if exec.Database != 7*time.Millisecond {
		t.Fatalf("database = %s, want 7ms", exec.Database)
	}
	if exec.Logic != 3*time.Millisecond {
		t.Fatalf("logic = %s, want 3ms (10ms query - 7ms db)", exec.Logic)
	}
	if len(exec.DatabaseOps) != 1 || exec.DatabaseOps[0].Name != "gorm:query" {
		t.Fatalf("database ops = %+v", exec.DatabaseOps)
	}
	if len(exec.Tags) != 1 || exec.Tags[0] != "messages_room_id:alpha" {
		t.Fatalf("tags = %v", exec.Tags)
	}
}

func TestSanitizeMetricsMutationSubtractsDBAndRouting(t *testing.T) {
	start := time.Now()
	report := SanitizeMetrics([]Metric{
		{
			ID:       "m1",
			Name:     "gorm:create",
			Type:     MetricTypeDatabase,
			Time:     start,
			Duration: 5 * time.Millisecond,
			Tags:     []string{"messages_room_id:alpha"},
		},
		{
			ID:       "m1",
			Name:     "batch_execution:getMessages",
			Type:     MetricTypeRouting,
			Time:     start.Add(5 * time.Millisecond),
			Duration: 8 * time.Millisecond,
			Value:    4,
		},
		{
			ID:       "m1",
			Name:     "batch_execution:getOther",
			Type:     MetricTypeRouting,
			Time:     start.Add(5 * time.Millisecond),
			Duration: 12 * time.Millisecond,
			Value:    2,
		},
		{
			ID:       "m1",
			Name:     "invalidate_tags:createMessage",
			Type:     MetricTypeRouting,
			Time:     start.Add(5 * time.Millisecond),
			Duration: 12 * time.Millisecond,
			Tags:     []string{"messages_room_id:alpha"},
		},
		{
			ID:       "m1",
			Name:     "mutation:createMessage",
			Type:     MetricTypeMutation,
			Time:     start,
			Duration: 20 * time.Millisecond,
		},
	})
	exec := report.Executions[0]
	if exec.Database != 5*time.Millisecond {
		t.Fatalf("database = %s, want 5ms", exec.Database)
	}
	if exec.Routing != 12*time.Millisecond {
		t.Fatalf("routing = %s, want 12ms from invalidate_tags (not summed batch_execution)", exec.Routing)
	}
	if exec.Logic != 3*time.Millisecond {
		t.Fatalf("logic = %s, want 3ms (20ms - 5ms db - 12ms routing)", exec.Logic)
	}
	if len(exec.RoutingOps) != 3 {
		t.Fatalf("routing ops = %d, want 3 (raw spans kept for exploration)", len(exec.RoutingOps))
	}
}

func TestSanitizeMetricsGroupsByExecutionID(t *testing.T) {
	start := time.Now()
	report := SanitizeMetrics([]Metric{
		{ID: "b", Name: "query:late", Type: MetricTypeQuery, Time: start.Add(time.Second), Duration: 2 * time.Millisecond},
		{ID: "a", Name: "query:early", Type: MetricTypeQuery, Time: start, Duration: 1 * time.Millisecond},
		{ID: "a", Name: "gorm:query", Type: MetricTypeDatabase, Time: start, Duration: 400 * time.Microsecond},
		{ID: "b", Name: "gorm:query", Type: MetricTypeDatabase, Time: start.Add(time.Second), Duration: 1500 * time.Microsecond},
	})
	if len(report.Executions) != 2 {
		t.Fatalf("executions = %d, want 2", len(report.Executions))
	}
	if report.Executions[0].ID != "a" || report.Executions[1].ID != "b" {
		t.Fatalf("order by start time: %s then %s", report.Executions[0].ID, report.Executions[1].ID)
	}
	if report.Executions[0].Logic != 600*time.Microsecond {
		t.Fatalf("a logic = %s, want 600µs", report.Executions[0].Logic)
	}
	if report.Executions[1].Logic != 500*time.Microsecond {
		t.Fatalf("b logic = %s, want 500µs", report.Executions[1].Logic)
	}
}

func TestSanitizeMetricsClampsLogicAndStripsAuthToken(t *testing.T) {
	start := time.Now()
	report := SanitizeMetrics([]Metric{
		{
			ID:       "q",
			Name:     "query:slow",
			Type:     MetricTypeQuery,
			Time:     start,
			Duration: 5 * time.Millisecond,
		},
		{
			ID:       "q",
			Name:     "gorm:query",
			Type:     MetricTypeDatabase,
			Time:     start,
			Duration: 8 * time.Millisecond,
		},
		{
			ID:       "auth",
			Name:     "authentication:super-secret-token",
			Type:     MetricTypeAuthentication,
			Time:     start,
			Duration: 2 * time.Millisecond,
		},
	})
	var query, auth *ProfiledExecution
	for i := range report.Executions {
		switch report.Executions[i].Type {
		case MetricTypeQuery:
			query = &report.Executions[i]
		case MetricTypeAuthentication:
			auth = &report.Executions[i]
		}
	}
	if query == nil || query.Logic != 0 {
		t.Fatalf("logic should clamp at 0 when db exceeds total, got %+v", query)
	}
	if auth == nil || auth.Name != "authentication" {
		t.Fatalf("auth name = %v, want sanitized 'authentication'", auth)
	}
	if strings.Contains(report.String(), "super-secret-token") {
		t.Fatal("report leaked authentication token")
	}
}

func TestSanitizeMetricsAggregatesAndSlowest(t *testing.T) {
	start := time.Now()
	report := SanitizeMetrics([]Metric{
		{ID: "1", Name: "query:getMessages", Type: MetricTypeQuery, Time: start, Duration: 10 * time.Millisecond},
		{ID: "1", Name: "gorm:query", Type: MetricTypeDatabase, Time: start, Duration: 4 * time.Millisecond},
		{ID: "2", Name: "query:getMessages", Type: MetricTypeQuery, Time: start.Add(time.Millisecond), Duration: 20 * time.Millisecond},
		{ID: "2", Name: "gorm:query", Type: MetricTypeDatabase, Time: start.Add(time.Millisecond), Duration: 8 * time.Millisecond},
		{ID: "3", Name: "mutation:createMessage", Type: MetricTypeMutation, Time: start.Add(2 * time.Millisecond), Duration: 5 * time.Millisecond},
	})
	if report.Totals.Executions != 3 {
		t.Fatalf("totals.Executions = %d, want 3", report.Totals.Executions)
	}
	if len(report.Aggregates) != 2 {
		t.Fatalf("aggregates = %d, want 2", len(report.Aggregates))
	}
	agg := report.Aggregates[0]
	if agg.Name != "query:getMessages" || agg.Count != 2 {
		t.Fatalf("first aggregate = %+v", agg)
	}
	if agg.TotalSum != 30*time.Millisecond || agg.TotalAvg != 15*time.Millisecond {
		t.Fatalf("query totals: sum=%s avg=%s", agg.TotalSum, agg.TotalAvg)
	}
	if agg.LogicSum != 18*time.Millisecond || agg.DatabaseAvg != 6*time.Millisecond {
		t.Fatalf("query components: logicSum=%s dbAvg=%s", agg.LogicSum, agg.DatabaseAvg)
	}
	slowest := report.Slowest(1)
	if len(slowest) != 1 || slowest[0].ID != "2" {
		t.Fatalf("slowest = %+v", slowest)
	}
}

func TestSanitizeMetricsUnattributedEmptyID(t *testing.T) {
	report := SanitizeMetrics([]Metric{
		{Name: "gorm:query", Type: MetricTypeDatabase, Duration: 3 * time.Millisecond},
		{Name: "gorm:query", Type: MetricTypeDatabase, Duration: 2 * time.Millisecond},
	})
	if len(report.Executions) != 1 {
		t.Fatalf("executions = %d, want 1 unattributed bucket", len(report.Executions))
	}
	exec := report.Executions[0]
	if exec.ID != unattributedExecutionID {
		t.Fatalf("id = %q, want %q", exec.ID, unattributedExecutionID)
	}
	if exec.Name != unattributedExecutionID || exec.Type != MetricTypeDatabase {
		t.Fatalf("unattributed identity: %+v", exec)
	}
	if exec.Database != 5*time.Millisecond || exec.Total != 5*time.Millisecond || exec.Logic != 0 {
		t.Fatalf("unattributed db-only: %+v", exec)
	}
	if len(exec.DatabaseOps) != 2 {
		t.Fatalf("database ops = %d, want 2 sibling spans", len(exec.DatabaseOps))
	}
}
