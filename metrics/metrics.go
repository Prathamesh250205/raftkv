// Package metrics is a minimal Prometheus exporter: counters, gauges and
// histograms rendered in the Prometheus text exposition format.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []uint64 // counts[i] = observations <= bounds[i]; last is +Inf
	sum    float64
}

func NewHistogram(bounds ...float64) *Histogram {
	return &Histogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

// ExponentialBuckets returns n bounds starting at start, each factor times the last.
func ExponentialBuckets(start, factor float64, n int) []float64 {
	b := make([]float64, n)
	for i := range b {
		b[i] = start * math.Pow(factor, float64(i))
	}
	return b
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
		}
	}
	h.counts[len(h.bounds)]++
}

type Labels map[string]string

func (l Labels) String() string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, l[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// With returns a copy of l with k set to v.
func (l Labels) With(k, v string) Labels {
	out := Labels{k: v}
	for lk, lv := range l {
		out[lk] = lv
	}
	return out
}

type family struct {
	help, typ string
	series    []func(w io.Writer, name string)
}

// Registry collects metric series and serves them at /metrics.
type Registry struct {
	mu       sync.Mutex
	names    []string
	families map[string]*family
}

func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

func (r *Registry) add(name, help, typ string, write func(io.Writer, string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		f = &family{help: help, typ: typ}
		r.families[name] = f
		r.names = append(r.names, name)
	}
	f.series = append(f.series, write)
}

func (r *Registry) Counter(name, help string, labels Labels, c *Counter) {
	r.add(name, help, "counter", func(w io.Writer, n string) {
		fmt.Fprintf(w, "%s%s %d\n", n, labels, c.Value())
	})
}

func (r *Registry) CounterFunc(name, help string, labels Labels, f func() float64) {
	r.add(name, help, "counter", func(w io.Writer, n string) {
		fmt.Fprintf(w, "%s%s %g\n", n, labels, f())
	})
}

func (r *Registry) Gauge(name, help string, labels Labels, f func() float64) {
	r.add(name, help, "gauge", func(w io.Writer, n string) {
		fmt.Fprintf(w, "%s%s %g\n", n, labels, f())
	})
}

func (r *Registry) Histogram(name, help string, labels Labels, h *Histogram) {
	r.add(name, help, "histogram", func(w io.Writer, n string) {
		h.mu.Lock()
		defer h.mu.Unlock()
		for i, b := range h.bounds {
			fmt.Fprintf(w, "%s_bucket%s %d\n", n, labels.With("le", fmt.Sprint(b)), h.counts[i])
		}
		total := h.counts[len(h.bounds)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", n, labels.With("le", "+Inf"), total)
		fmt.Fprintf(w, "%s_sum%s %g\n", n, labels, h.sum)
		fmt.Fprintf(w, "%s_count%s %d\n", n, labels, total)
	})
}

// Write renders every metric in the Prometheus text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range r.names {
		f := r.families[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, f.help, name, f.typ)
		for _, s := range f.series {
			s(w, name)
		}
	}
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	r.Write(w)
}
