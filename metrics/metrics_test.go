package metrics

import (
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	var c Counter
	c.Add(3)
	h := NewHistogram(0.1, 1)
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(5)

	r.Counter("requests_total", "Requests.", Labels{"op": "get"}, &c)
	r.Gauge("term", "Current term.", nil, func() float64 { return 7 })
	r.Histogram("latency_seconds", "Latency.", Labels{"op": "get"}, h)

	var b strings.Builder
	r.Write(&b)
	for _, want := range []string{
		"# TYPE requests_total counter",
		`requests_total{op="get"} 3`,
		"term 7",
		`latency_seconds_bucket{le="0.1",op="get"} 1`,
		`latency_seconds_bucket{le="1",op="get"} 2`,
		`latency_seconds_bucket{le="+Inf",op="get"} 3`,
		`latency_seconds_count{op="get"} 3`,
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q in:\n%s", want, b.String())
		}
	}
}
