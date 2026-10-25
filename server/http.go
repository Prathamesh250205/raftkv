package server

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"raftkv/kv"
	"raftkv/metrics"
	"raftkv/raft"
)

const maxValueBytes = 1 << 20

// registerRoutes adds the HTTP API:
//
//	GET    /kv/{key}                  read a value (404 if missing)
//	PUT    /kv/{key}                  write the request body as the value
//	POST   /kv/{key}/append           append the request body to the value
//	DELETE /kv/{key}                  remove a key
//
//	GET    /status                    this node's Raft state for every shard
//	GET    /shards                    leader and members of every shard
//	GET    /shards/{g}/members        shard g's configuration
//	POST   /shards/{g}/members        add a node: {"id": 4, "addr": "host:port"}
//	DELETE /shards/{g}/members/{id}   remove a node
//	GET    /metrics                   Prometheus metrics
//
// A key's request goes to the leader of the shard the key hashes to; any
// other node answers 307 with that leader's address, or passes the request on
// itself in Forward mode. Optional headers X-Client-ID and X-Request-Seq make
// retried writes idempotent.
//
// With an AuthToken configured, writes and membership changes need it in the
// X-Api-Token header, and so do reads unless PublicReads is set.
func (n *Node) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /kv/{key}", n.requireTokenUnlessPublic(n.kv(kv.OpGet)))
	mux.HandleFunc("PUT /kv/{key}", n.requireToken(n.kv(kv.OpPut)))
	mux.HandleFunc("POST /kv/{key}/append", n.requireToken(n.kv(kv.OpAppend)))
	mux.HandleFunc("DELETE /kv/{key}", n.requireToken(n.kv(kv.OpDelete)))
	mux.HandleFunc("GET /status", n.status)
	mux.HandleFunc("GET /shards", n.shardMap)
	mux.HandleFunc("GET /shards/{g}/members", n.withShard(n.members))
	mux.HandleFunc("POST /shards/{g}/members", n.requireToken(n.withShard(n.addMember)))
	mux.HandleFunc("DELETE /shards/{g}/members/{id}", n.requireToken(n.withShard(n.removeMember)))
	mux.Handle("GET /metrics", n.reg)
}

// Request outcomes, used as the "result" label on kv_requests_total.
const (
	resultOK          = "ok"
	resultNotFound    = "not_found"
	resultRedirect    = "redirect"
	resultForwarded   = "forwarded"
	resultUnavailable = "unavailable"
	resultBadRequest  = "bad_request"
)

func (n *Node) kv(op kv.Op) http.HandlerFunc {
	// Pre-register every series so dashboards see zeros instead of gaps.
	counts := map[string]*metrics.Counter{}
	for _, res := range []string{resultOK, resultNotFound, resultRedirect, resultForwarded, resultUnavailable, resultBadRequest} {
		counts[res] = &metrics.Counter{}
		n.reg.Counter("kv_requests_total", "Client requests by operation and outcome.",
			metrics.Labels{"op": string(op), "result": res}, counts[res])
	}
	latency := metrics.NewHistogram(metrics.ExponentialBuckets(0.0005, 2, 14)...)
	n.reg.Histogram("kv_request_seconds", "Client request latency, including replication.",
		metrics.Labels{"op": string(op)}, latency)

	return func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		result := n.serve(op, w, req)
		counts[result].Inc()
		if result == resultOK || result == resultNotFound {
			latency.Observe(time.Since(start).Seconds())
		}
	}
}

func (n *Node) serve(op kv.Op, w http.ResponseWriter, req *http.Request) string {
	cmd := kv.Command{Op: op, Key: req.PathValue("key"), ClientID: req.Header.Get("X-Client-ID")}
	if seq := req.Header.Get("X-Request-Seq"); seq != "" {
		v, err := strconv.ParseUint(seq, 10, 64)
		if err != nil {
			http.Error(w, "invalid X-Request-Seq", http.StatusBadRequest)
			return resultBadRequest
		}
		cmd.Seq = v
	}
	if op == kv.OpPut || op == kv.OpAppend {
		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxValueBytes))
		if err != nil {
			http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			return resultBadRequest
		}
		cmd.Value = string(body)
	}

	sh := n.shardFor(cmd.Key)
	var res kv.Result
	var err error
	if op == kv.OpGet {
		res, err = sh.store.Get(cmd.Key)
	} else {
		res, err = sh.store.Do(cmd)
	}
	switch {
	case errors.Is(err, kv.ErrNotLeader):
		return n.notLeader(sh, w, req, []byte(cmd.Value))
	case err != nil:
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return resultUnavailable
	}

	if op == kv.OpGet {
		if !res.Found {
			http.Error(w, "key not found", http.StatusNotFound)
			return resultNotFound
		}
		io.WriteString(w, res.Value)
		return resultOK
	}
	w.WriteHeader(http.StatusNoContent)
	return resultOK
}

// redirect sends the client to shard sh's leader. If this node doesn't know
// the leader (for example it doesn't host the shard), it points at some node
// that should, and that node redirects onward.
func (n *Node) redirect(sh *shard, w http.ResponseWriter, req *http.Request) string {
	addr, ok := sh.raft.LeaderAddr()
	if ok && addr != n.cfg.Addr {
		w.Header().Set("X-Raft-Leader", addr)
	} else {
		candidates := n.peersFor(sh)
		if len(candidates) == 0 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "no leader known for this shard yet", http.StatusServiceUnavailable)
			return resultUnavailable
		}
		addr = candidates[rand.Intn(len(candidates))]
	}
	// 307 keeps the method and body, so clients that follow redirects just work.
	http.Redirect(w, req, "http://"+addr+req.URL.RequestURI(), http.StatusTemporaryRedirect)
	return resultRedirect
}

// peersFor lists other nodes likely to know shard sh's leader: its members
// and founders, or failing that every node this node knows through any shard
// (a node that joined later may host only some shards).
func (n *Node) peersFor(sh *shard) []string {
	collect := func(shards []*shard) []string {
		seen := map[string]bool{n.cfg.Addr: true}
		var out []string
		for _, s := range shards {
			for _, set := range []map[int]string{s.raft.Members(), s.founders} {
				for _, a := range set {
					if !seen[a] {
						seen[a] = true
						out = append(out, a)
					}
				}
			}
		}
		return out
	}
	if peers := collect([]*shard{sh}); len(peers) > 0 {
		return peers
	}
	return collect(n.shards)
}

type shardStatus struct {
	Shard int `json:"shard"`
	raft.Status
}

func (n *Node) status(w http.ResponseWriter, _ *http.Request) {
	out := struct {
		ID     int           `json:"id"`
		Addr   string        `json:"addr"`
		Shards []shardStatus `json:"shards"`
	}{ID: n.cfg.ID, Addr: n.cfg.Addr}
	for _, sh := range n.shards {
		out.Shards = append(out.Shards, shardStatus{sh.id, sh.raft.Status()})
	}
	writeJSON(w, out)
}

// ShardInfo is one entry of GET /shards: where to send a shard's requests.
type ShardInfo struct {
	Shard      int            `json:"shard"`
	LeaderID   int            `json:"leader_id"`
	LeaderAddr string         `json:"leader_addr,omitempty"`
	Members    map[int]string `json:"members"`
}

func (n *Node) shardMap(w http.ResponseWriter, req *http.Request) {
	var out []ShardInfo
	for _, sh := range n.shards {
		out = append(out, n.shardInfo(sh, req.URL.Query().Get("local") == ""))
	}
	writeJSON(w, out)
}

// shardInfo reports where shard sh's leader is. A node that doesn't host the
// shard (or hasn't heard from its leader) asks a node that should know, once;
// local=1 on that query stops it being forwarded again.
func (n *Node) shardInfo(sh *shard, forward bool) ShardInfo {
	st := sh.raft.Status()
	info := ShardInfo{Shard: sh.id, LeaderID: st.LeaderID, Members: st.Members}
	if addr, ok := sh.raft.LeaderAddr(); ok {
		info.LeaderAddr = addr
		return info
	}
	if !forward {
		return info
	}
	for _, addr := range n.peersFor(sh) {
		if remote, ok := n.askShard(addr, sh.id); ok && remote.LeaderAddr != "" {
			return remote
		}
	}
	return info
}

var peerClient = &http.Client{Timeout: 500 * time.Millisecond}

func (n *Node) askShard(addr string, g int) (ShardInfo, bool) {
	resp, err := peerClient.Get("http://" + addr + "/shards/" + strconv.Itoa(g) + "/members?local=1")
	if err != nil {
		return ShardInfo{}, false
	}
	defer resp.Body.Close()
	var info ShardInfo
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&info) != nil {
		return ShardInfo{}, false
	}
	return info, true
}

func (n *Node) withShard(f func(*shard, http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		g, err := strconv.Atoi(req.PathValue("g"))
		if err != nil || g < 0 || g >= len(n.shards) {
			http.Error(w, "no such shard", http.StatusNotFound)
			return
		}
		f(n.shards[g], w, req)
	}
}

func (n *Node) members(sh *shard, w http.ResponseWriter, req *http.Request) {
	writeJSON(w, n.shardInfo(sh, req.URL.Query().Get("local") == ""))
}

func (n *Node) addMember(sh *shard, w http.ResponseWriter, req *http.Request) {
	raw, _ := io.ReadAll(io.LimitReader(req.Body, 4096))
	var body struct {
		ID   int    `json:"id"`
		Addr string `json:"addr"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Addr == "" {
		http.Error(w, `want JSON {"id": <int>, "addr": "host:port"}`, http.StatusBadRequest)
		return
	}
	idx, err := sh.raft.AddMember(body.ID, body.Addr)
	n.finishChange(sh, w, req, raw, idx, err)
}

func (n *Node) removeMember(sh *shard, w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid member id", http.StatusBadRequest)
		return
	}
	idx, err := sh.raft.RemoveMember(id)
	n.finishChange(sh, w, req, nil, idx, err)
}

// finishChange waits for a membership change to commit and reports the result.
func (n *Node) finishChange(sh *shard, w http.ResponseWriter, req *http.Request, body []byte, idx int, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		n.notLeader(sh, w, req, body)
		return
	case errors.Is(err, raft.ErrAlreadyMember), errors.Is(err, raft.ErrNotMember), errors.Is(err, raft.ErrLastMember):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil: // not ready yet, or another change in progress
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if sh.raft.Status().CommitIndex >= idx {
			n.members(sh, w, req)
			return
		}
	}
	http.Error(w, "membership change did not commit in time; check GET /shards", http.StatusGatewayTimeout)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
