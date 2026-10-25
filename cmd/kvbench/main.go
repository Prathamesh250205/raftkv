// Command kvbench measures throughput and latency against a running cluster.
//
//	kvbench -addr 127.0.0.1:8001 -workers 32 -duration 10s -op put
//	kvbench -addr 127.0.0.1:8001 -op get
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8001", "any node; followers redirect to the leader")
	workers := flag.Int("workers", 32, "concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	op := flag.String("op", "put", "put (writes) or get (linearizable reads)")
	flag.Parse()
	if *op != "put" && *op != "get" {
		log.Fatalf("unknown -op %q", *op)
	}

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *workers},
	}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		errCount  int
		wg        sync.WaitGroup
	)
	deadline := time.Now().Add(*duration)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				url := fmt.Sprintf("http://%s/kv/bench-%d-%d", *addr, w, i)
				var req *http.Request
				if *op == "put" {
					req, _ = http.NewRequest(http.MethodPut, url, strings.NewReader("value"))
				} else {
					req, _ = http.NewRequest(http.MethodGet, url, nil)
				}
				start := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(start)
				ok := err == nil && (resp.StatusCode == http.StatusNoContent ||
					resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				mu.Lock()
				if ok {
					latencies = append(latencies, elapsed)
				} else {
					errCount++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(latencies) == 0 {
		log.Fatalf("no successful requests (%d errors); is the cluster running?", errCount)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
	fmt.Printf("%-11s %d ok, %d errors\n", *op+"s:", len(latencies), errCount)
	fmt.Printf("throughput: %.0f ops/sec\n", float64(len(latencies))/duration.Seconds())
	fmt.Printf("latency:    p50 %v  p95 %v  p99 %v\n", pct(0.50), pct(0.95), pct(0.99))
}
