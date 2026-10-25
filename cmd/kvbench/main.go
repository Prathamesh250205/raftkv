// Command kvbench measures throughput and latency against a running cluster.
// It asks the cluster for each shard's leader and sends every key straight
// there, like a smart client; stale routes still work via redirects.
//
//	kvbench -addr 127.0.0.1:8001 -workers 32 -duration 10s -op put
//	kvbench -addr 127.0.0.1:8001 -op get
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"raftkv/kv"
	"raftkv/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8001", "any node of the cluster")
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
	routes := leaders(client, *addr)
	fmt.Printf("shards:     %d, leaders %v\n", len(routes), routes)

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
				key := fmt.Sprintf("bench-%d-%d", w, i)
				url := fmt.Sprintf("http://%s/kv/%s", routes[kv.ShardFor(key, len(routes))], key)
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

// leaders returns, per shard, the address requests should go to. It waits
// until every shard has elected a leader.
func leaders(client *http.Client, addr string) []string {
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		resp, err := client.Get("http://" + addr + "/shards")
		if err != nil {
			log.Fatalf("cannot reach %s: %v", addr, err)
		}
		var shards []server.ShardInfo
		err = json.NewDecoder(resp.Body).Decode(&shards)
		resp.Body.Close()
		if err != nil {
			log.Fatalf("bad /shards response: %v", err)
		}
		out := make([]string, len(shards))
		ready := true
		for _, s := range shards {
			out[s.Shard] = s.LeaderAddr
			ready = ready && s.LeaderAddr != ""
		}
		if ready {
			return out
		}
		if time.Now().After(deadline) {
			log.Fatal("timed out waiting for every shard to elect a leader")
		}
	}
}
