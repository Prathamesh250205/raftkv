package kv

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

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
// Followers answer writes and reads with a 307 redirect to the leader.
// Optional headers X-Client-ID and X-Request-Seq make retried writes idempotent.
func RegisterRoutes(mux *http.ServeMux, s *Store, r *raft.Raft, addrs map[int]string) {
	h := &handler{store: s, raft: r, addrs: addrs}
	mux.HandleFunc("GET /kv/{key}", h.kv(OpGet))
	mux.HandleFunc("PUT /kv/{key}", h.kv(OpPut))
	mux.HandleFunc("POST /kv/{key}/append", h.kv(OpAppend))
	mux.HandleFunc("DELETE /kv/{key}", h.kv(OpDelete))
	mux.HandleFunc("GET /status", h.status)
}

type handler struct {
	store *Store
	raft  *raft.Raft
	addrs map[int]string
}

func (h *handler) kv(op Op) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		cmd := Command{Op: op, Key: req.PathValue("key"), ClientID: req.Header.Get("X-Client-ID")}
		if seq := req.Header.Get("X-Request-Seq"); seq != "" {
			n, err := strconv.ParseUint(seq, 10, 64)
			if err != nil {
				http.Error(w, "invalid X-Request-Seq", http.StatusBadRequest)
				return
			}
			cmd.Seq = n
		}
		if op == OpPut || op == OpAppend {
			body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxValueBytes))
			if err != nil {
				http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
				return
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
			h.redirectToLeader(w, req)
			return
		case err != nil:
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		switch op {
		case OpGet:
			if !res.Found {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			io.WriteString(w, res.Value)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func (h *handler) redirectToLeader(w http.ResponseWriter, req *http.Request) {
	addr, ok := h.addrs[h.raft.Status().LeaderID]
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("X-Raft-Leader", addr)
	// 307 keeps the method and body, so clients that follow redirects just work.
	http.Redirect(w, req, "http://"+addr+req.URL.RequestURI(), http.StatusTemporaryRedirect)
}

func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.raft.Status())
}
