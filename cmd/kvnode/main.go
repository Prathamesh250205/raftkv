// Command kvnode runs one node of the sharded, replicated key-value store.
//
// Bootstrap a new cluster by giving every founding node the same -cluster
// and -shards:
//
//	kvnode -id 1 -shards 4 -cluster 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
//
// Grow it later by starting a node with -join and adding it to shards with
// kvctl (or POST /shards/{g}/members):
//
//	kvnode -id 4 -addr 127.0.0.1:8004 -shards 4 -join
//	kvctl -addr 127.0.0.1:8001 add-node 4 127.0.0.1:8004
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"strings"

	"raftkv/server"
)

func main() {
	id := flag.Int("id", 1, "this node's ID")
	addr := flag.String("addr", "", "this node's host:port as other nodes reach it (default: its entry in -cluster)")
	listen := flag.String("listen", "", "host:port to listen on, if different from -addr (e.g. 0.0.0.0:8000 in a container)")
	cluster := flag.String("cluster", "1=127.0.0.1:8001", "founding nodes as id=host:port,...; ignored once a shard has data")
	join := flag.Bool("join", false, "start empty and wait to be added to an existing cluster")
	shards := flag.Int("shards", 1, "number of Raft groups to hash keys across (same on every node)")
	replicas := flag.Int("replicas", 0, "founding nodes per shard (0 = every node)")
	dataDir := flag.String("data", "", "directory for durable state (default data/node<id>)")
	snapshotEvery := flag.Int("snapshot-every", 1000, "compact the log after this many applied entries (0 disables)")
	logReads := flag.Bool("log-reads", false, "serve reads through the Raft log instead of ReadIndex (for comparison)")
	wal := flag.String("wal", "shared", "shared: one log for all shards on this node; files: a file set per shard (for comparison)")
	flag.Parse()

	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.Ltime|log.Lmicroseconds)

	cfg := server.Config{
		ID:            *id,
		Addr:          *addr,
		Shards:        *shards,
		Replicas:      *replicas,
		DataDir:       *dataDir,
		SnapshotEvery: *snapshotEvery,
		LogReads:      *logReads,
		SeparateFiles: *wal == "files",
		Logger:        logger,
	}
	if *wal != "shared" && *wal != "files" {
		logger.Fatalf("unknown -wal %q", *wal)
	}
	if !*join {
		var err error
		if cfg.Cluster, err = parseCluster(*cluster); err != nil {
			logger.Fatal(err)
		}
	}
	if cfg.Addr == "" {
		if cfg.Addr = cfg.Cluster[*id]; cfg.Addr == "" {
			logger.Fatalf("no address for node %d: pass -addr, or list it in -cluster", *id)
		}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = fmt.Sprintf("data/node%d", *id)
	}
	if *listen != "" {
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			logger.Fatal(err)
		}
		cfg.Listener = ln
	}

	node, err := server.Start(cfg)
	if err != nil {
		logger.Fatal(err)
	}
	logger.Printf("listening on %s with %d shard(s), data in %s", cfg.Addr, cfg.Shards, cfg.DataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM) // SIGTERM: docker stop
	<-sig
	logger.Print("shutting down")
	node.Close()
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
