package kv

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"raftkv/metrics"
	"raftkv/raft"
)

const maxValueBytes = 1 << 20

// RegisterRoutes adds the client API to mux:
//
//	GET    /kv/{key}   read a value (404 if missing)
//	PUT    /kv/{key}   write the request body as the value
//	POST   /kv/{key}/append   append the request body to the value
//	DELETE /kv/{key}   remove a key
//	GET    /status     this node's Raft state
//
//	GET    /cluster/members        current configuration
//	POST   /cluster/members        add a node: {"id": 4, "addr": "host:port"}
//	DELETE /cluster/members/{id}   remove a node
//
// Followers answer with a 307 redirect to the leader.
// Optional headers X-Client-ID and X-Request-Seq make retried writes idempotent.
// Request counts and latencies are exported on reg.
func RegisterRoutes(mux *http.ServeMux, s *Store, r *raft.Raft, reg *metrics.Registry) {
	h := &handler{store: s, raft: r, reg: reg}
	mux.HandleFunc("GET /kv/{key}", h.kv(OpGet))
	mux.HandleFunc("PUT /kv/{key}", h.kv(OpPut))
	mux.HandleFunc("POST /kv/{key}/append", h.kv(OpAppend))
	mux.HandleFunc("DELETE /kv/{key}", h.kv(OpDelete))
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /cluster/members", h.members)
	mux.HandleFunc("POST /cluster/members", h.addMember)
	mux.HandleFunc("DELETE /cluster/members/{id}", h.removeMember)
}

type handler struct {
	store *Store
	raft  *raft.Raft
	reg   *metrics.Registry
}

// Request outcomes, used as the "result" label on kv_requests_total.
const (
	resultOK          = "ok"
	resultNotFound    = "not_found"
	resultRedirect    = "redirect"
	resultUnavailable = "unavailable"
	resultBadRequest  = "bad_request"
)

func (h *handler) kv(op Op) http.HandlerFunc {
	// Pre-register every series so dashboards see zeros instead of gaps.
	counts := map[string]*metrics.Counter{}
	for _, res := range []string{resultOK, resultNotFound, resultRedirect, resultUnavailable, resultBadRequest} {
		counts[res] = &metrics.Counter{}
		h.reg.Counter("kv_requests_total", "Client requests by operation and outcome.",
			metrics.Labels{"op": string(op), "result": res}, counts[res])
	}
	latency := metrics.NewHistogram(metrics.ExponentialBuckets(0.0005, 2, 14)...)
	h.reg.Histogram("kv_request_seconds", "Client request latency, including replication.",
		metrics.Labels{"op": string(op)}, latency)

	return func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		result := h.serve(op, w, req)
		counts[result].Inc()
		if result == resultOK || result == resultNotFound {
			latency.Observe(time.Since(start).Seconds())
		}
	}
}

func (h *handler) serve(op Op, w http.ResponseWriter, req *http.Request) string {
	cmd := Command{Op: op, Key: req.PathValue("key"), ClientID: req.Header.Get("X-Client-ID")}
	if seq := req.Header.Get("X-Request-Seq"); seq != "" {
		n, err := strconv.ParseUint(seq, 10, 64)
		if err != nil {
			http.Error(w, "invalid X-Request-Seq", http.StatusBadRequest)
			return resultBadRequest
		}
		cmd.Seq = n
	}
	if op == OpPut || op == OpAppend {
		body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxValueBytes))
		if err != nil {
			http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			return resultBadRequest
		}
		cmd.Value = string(body)
	}

	var res Result
	var err error
	if op == OpGet {
		res, err = h.store.Get(cmd.Key)
	} else {
		res, err = h.store.Do(cmd)
	}
	switch {
	case errors.Is(err, ErrNotLeader):
		return h.redirectToLeader(w, req)
	case err != nil:
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return resultUnavailable
	}

	if op == OpGet {
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

func (h *handler) redirectToLeader(w http.ResponseWriter, req *http.Request) string {
	addr, ok := h.raft.LeaderAddr()
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
		return resultUnavailable
	}
	w.Header().Set("X-Raft-Leader", addr)
	// 307 keeps the method and body, so clients that follow redirects just work.
	http.Redirect(w, req, "http://"+addr+req.URL.RequestURI(), http.StatusTemporaryRedirect)
	return resultRedirect
}

func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.raft.Status())
}

func (h *handler) members(w http.ResponseWriter, _ *http.Request) {
	st := h.raft.Status()
	writeJSON(w, map[string]any{"leader_id": st.LeaderID, "members": st.Members})
}

func (h *handler) addMember(w http.ResponseWriter, req *http.Request) {
	var body struct {
		ID   int    `json:"id"`
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(io.LimitReader(req.Body, 4096)).Decode(&body); err != nil || body.Addr == "" {
		http.Error(w, `want JSON {"id": <int>, "addr": "host:port"}`, http.StatusBadRequest)
		return
	}
	idx, err := h.raft.AddMember(body.ID, body.Addr)
	h.finishChange(w, req, idx, err)
}

func (h *handler) removeMember(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid member id", http.StatusBadRequest)
		return
	}
	idx, err := h.raft.RemoveMember(id)
	h.finishChange(w, req, idx, err)
}

// finishChange waits for a membership change to commit and reports the result.
func (h *handler) finishChange(w http.ResponseWriter, req *http.Request, idx int, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		h.redirectToLeader(w, req)
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
		if h.raft.Status().CommitIndex >= idx {
			h.members(w, req)
			return
		}
	}
	http.Error(w, "membership change did not commit in time; check /cluster/members", http.StatusGatewayTimeout)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
