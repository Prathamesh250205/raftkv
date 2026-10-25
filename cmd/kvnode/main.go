// Command kvnode runs one node of the replicated key-value store.
//
//	kvnode -id 1 -cluster 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"

	"raftkv/kv"
	"raftkv/raft"
)

func main() {
	id := flag.Int("id", 1, "this node's ID (must appear in -cluster)")
	cluster := flag.String("cluster", "1=127.0.0.1:8001", "comma-separated id=host:port for every node")
	dataDir := flag.String("data", "", "directory for durable state (default data/node<id>)")
	snapshotEvery := flag.Int("snapshot-every", 1000, "compact the log after this many applied entries (0 disables)")
	logReads := flag.Bool("log-reads", false, "serve reads through the Raft log instead of ReadIndex (for comparison)")
	flag.Parse()

	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.Ltime|log.Lmicroseconds)

	addrs, err := parseCluster(*cluster)
	if err != nil {
		logger.Fatal(err)
	}
	self, ok := addrs[*id]
	if !ok {
		logger.Fatalf("id %d not found in -cluster %q", *id, *cluster)
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/node%d", *id)
	}

	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		logger.Fatal(err)
	}
	peers := make([]int, 0, len(addrs))
	for p := range addrs {
		peers = append(peers, p)
	}
	sort.Ints(peers)

	applyCh := make(chan raft.ApplyMsg, 256)
	r, err := raft.New(raft.Config{
		ID:        *id,
		Peers:     peers,
		Transport: raft.NewHTTPTransport(addrs),
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
	mux := http.NewServeMux()
	mux.Handle(raft.RPCPath, rpcHandler)
	kv.RegisterRoutes(mux, store, r, addrs)

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
