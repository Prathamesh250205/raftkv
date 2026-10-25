// Command kvnode runs one node of the replicated key-value store.
//
// Bootstrap a new cluster by giving every founding node the same -cluster:
//
//	kvnode -id 1 -cluster 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
//
// Grow it later by starting a node with -join and adding it through the API:
//
//	kvnode -id 4 -addr 127.0.0.1:8004 -join
//	curl -L -X POST -d '{"id":4,"addr":"127.0.0.1:8004"}' http://127.0.0.1:8001/cluster/members
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"raftkv/kv"
	"raftkv/metrics"
	"raftkv/raft"
)

func main() {
	id := flag.Int("id", 1, "this node's ID")
	addr := flag.String("addr", "", "this node's host:port (default: its entry in -cluster)")
	cluster := flag.String("cluster", "1=127.0.0.1:8001", "founding members as id=host:port,...; ignored once the node has data")
	join := flag.Bool("join", false, "start empty and wait to be added to an existing cluster")
	dataDir := flag.String("data", "", "directory for durable state (default data/node<id>)")
	snapshotEvery := flag.Int("snapshot-every", 1000, "compact the log after this many applied entries (0 disables)")
	logReads := flag.Bool("log-reads", false, "serve reads through the Raft log instead of ReadIndex (for comparison)")
	flag.Parse()

	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.Ltime|log.Lmicroseconds)

	var peers map[int]string
	if !*join {
		var err error
		if peers, err = parseCluster(*cluster); err != nil {
			logger.Fatal(err)
		}
	}
	self := *addr
	if self == "" {
		if self = peers[*id]; self == "" {
			logger.Fatalf("no address for node %d: pass -addr, or list it in -cluster", *id)
		}
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node%d", *id)
	}

	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		logger.Fatal(err)
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	r, err := raft.New(raft.Config{
		ID:        *id,
		Peers:     peers,
		Transport: raft.NewHTTPTransport(peers),
		Persister: persister,
		ApplyCh:   applyCh,
		Logger:    logger,
	})
	if err != nil {
		logger.Fatal(err)
	}
	store := kv.NewStore(r, applyCh, *snapshotEvery)
	store.LogReads = *logReads

	rpcHandler, err := raft.NewRPCHandler(r)
	if err != nil {
		logger.Fatal(err)
	}
	reg := metrics.NewRegistry()
	r.RegisterMetrics(reg, metrics.Labels{"node": strconv.Itoa(*id)})

	mux := http.NewServeMux()
	mux.Handle(raft.RPCPath, rpcHandler)
	mux.Handle("GET /metrics", reg)
	kv.RegisterRoutes(mux, store, r, reg)

	srv := &http.Server{Addr: self, Handler: mux}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		logger.Print("shutting down")
		r.Stop()
		srv.Close()
	}()

	logger.Printf("listening on %s, data in %s", self, *dataDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal(err)
	}
}

func parseCluster(s string) (map[int]string, error) {
	addrs := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad -cluster entry %q, want id=host:port", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("bad node id in %q: %v", part, err)
		}
		addrs[id] = addr
	}
	return addrs, nil
}
