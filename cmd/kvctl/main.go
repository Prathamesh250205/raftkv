// Command kvctl administers a running cluster through any of its nodes.
//
//	kvctl -addr 127.0.0.1:8001 status
//	kvctl -addr 127.0.0.1:8001 add-node 4 127.0.0.1:8004        # every shard
//	kvctl -addr 127.0.0.1:8001 -shard 2 add-node 4 127.0.0.1:8004
//	kvctl -addr 127.0.0.1:8001 remove-node 1
//	kvctl -addr 127.0.0.1:8001 move-shard 2 1 4 127.0.0.1:8004  # shard 2: node 1 -> node 4
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"raftkv/server"
)

var (
	addr   = flag.String("addr", "127.0.0.1:8001", "any node of the cluster: host:port or a full URL such as https://kv.example.com")
	token  = flag.String("token", os.Getenv("RAFTKV_AUTH_TOKEN"), "API token for membership changes (default $RAFTKV_AUTH_TOKEN)")
	only   = flag.Int("shard", -1, "limit add-node/remove-node to one shard")
	client = &http.Client{Timeout: 10 * time.Second}
)

// baseURL turns -addr into a URL prefix, defaulting to plain HTTP.
func baseURL() string {
	if strings.Contains(*addr, "://") {
		return strings.TrimSuffix(*addr, "/")
	}
	return "http://" + *addr
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr host:port] [-shard g] status | add-node ID ADDR | remove-node ID | move-shard G FROM TO TOADDR")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var err error
	switch {
	case args[0] == "status" && len(args) == 1:
		err = status()
	case args[0] == "add-node" && len(args) == 3:
		err = forShards(func(g int) error { return add(g, atoi(args[1]), args[2]) })
	case args[0] == "remove-node" && len(args) == 2:
		err = forShards(func(g int) error { return remove(g, atoi(args[1])) })
	case args[0] == "move-shard" && len(args) == 5:
		g, from, to := atoi(args[1]), atoi(args[2]), atoi(args[3])
		// Add first, then remove: the shard never drops below its replica count.
		if err = add(g, to, args[4]); err == nil {
			err = remove(g, from)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

func status() error {
	shards, err := shardMap()
	if err != nil {
		return err
	}
	fmt.Printf("%-6s %-8s %s\n", "SHARD", "LEADER", "MEMBERS")
	for _, s := range shards {
		ids := make([]int, 0, len(s.Members))
		for id := range s.Members {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		leader := "-"
		if s.LeaderID >= 0 {
			leader = strconv.Itoa(s.LeaderID)
		}
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = fmt.Sprintf("%d=%s", id, s.Members[id])
		}
		fmt.Printf("%-6d %-8s %s\n", s.Shard, leader, strings.Join(parts, " "))
	}
	return nil
}

func forShards(f func(g int) error) error {
	if *only >= 0 {
		return f(*only)
	}
	shards, err := shardMap()
	if err != nil {
		return err
	}
	for _, s := range shards {
		if err := f(s.Shard); err != nil {
			return err
		}
	}
	return nil
}

func add(g, id int, nodeAddr string) error {
	body, _ := json.Marshal(map[string]any{"id": id, "addr": nodeAddr})
	return change(g, http.MethodPost, fmt.Sprintf("/shards/%d/members", g), body,
		fmt.Sprintf("added node %d to shard %d", id, g))
}

func remove(g, id int) error {
	return change(g, http.MethodDelete, fmt.Sprintf("/shards/%d/members/%d", g, id), nil,
		fmt.Sprintf("removed node %d from shard %d", id, g))
}

// change sends a membership request, retrying while the shard is busy with
// another change or electing a leader. Redirects to the leader are followed.
func change(g int, method, path string, body []byte, done string) error {
	for attempt := 0; ; attempt++ {
		req, _ := http.NewRequest(method, baseURL()+path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(server.TokenHeader, *token)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK:
			fmt.Println(done)
			return nil
		case resp.StatusCode == http.StatusServiceUnavailable && attempt < 50:
			time.Sleep(200 * time.Millisecond)
		default:
			return fmt.Errorf("shard %d: %s %s: %s", g, method, path, strings.TrimSpace(string(msg)))
		}
	}
}

func shardMap() ([]server.ShardInfo, error) {
	resp, err := client.Get(baseURL() + "/shards")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []server.ShardInfo
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvctl: %q is not a number\n", s)
		os.Exit(2)
	}
	return n
}
