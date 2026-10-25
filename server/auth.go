package server

import (
	"bytes"
	"crypto/subtle"
	"io"
	"net/http"
	"time"

	"raftkv/raft"
)

const (
	// TokenHeader carries the API token. A custom header rather than
	// Authorization, because HTTP clients drop Authorization when a redirect
	// goes to another host, and requests here often hop to a shard leader.
	TokenHeader = "X-Api-Token"

	// forwardedHeader marks a request one node passed on to another, so a
	// request never hops more than once.
	forwardedHeader = "X-Raftkv-Forwarded"
)

// requireToken rejects requests without the API token, if one is configured.
func (n *Node) requireToken(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if n.cfg.AuthToken != "" && !equal(req.Header.Get(TokenHeader), n.cfg.AuthToken) {
			http.Error(w, "missing or wrong "+TokenHeader, http.StatusUnauthorized)
			return
		}
		h(w, req)
	}
}

// requireTokenUnlessPublic is requireToken for reads: they stay open when
// PublicReads is set, so a demo can be browsed without the token.
func (n *Node) requireTokenUnlessPublic(h http.HandlerFunc) http.HandlerFunc {
	if n.cfg.PublicReads {
		return h
	}
	return n.requireToken(h)
}

// guardRPC lets only nodes holding the cluster secret make Raft RPCs. The
// secret travels in the HTTP CONNECT request that opens each connection.
func (n *Node) guardRPC(h http.Handler) http.Handler {
	if n.cfg.ClusterSecret == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !equal(req.Header.Get(raft.SecretHeader), n.cfg.ClusterSecret) {
			http.Error(w, "wrong cluster secret", http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, req)
	})
}

func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

var forwardClient = &http.Client{
	Timeout:       5 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// notLeader answers a request this node can't serve for shard sh: either a
// 307 to the leader, or, in Forward mode, by passing it to the leader and
// relaying the response. Forwarding suits deployments where nodes talk over
// private addresses that clients outside can't reach.
func (n *Node) notLeader(sh *shard, w http.ResponseWriter, req *http.Request, body []byte) string {
	if !n.cfg.Forward {
		return n.redirect(sh, w, req)
	}
	addr, ok := sh.raft.LeaderAddr()
	if !ok || addr == n.cfg.Addr || req.Header.Get(forwardedHeader) != "" {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader for this shard right now; retry", http.StatusServiceUnavailable)
		return resultUnavailable
	}

	out, err := http.NewRequest(req.Method, "http://"+addr+req.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return resultUnavailable
	}
	for k, vs := range req.Header {
		out.Header[k] = vs
	}
	out.Header.Set(forwardedHeader, n.cfg.Addr)
	resp, err := forwardClient.Do(out)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "leader unreachable; retry", http.StatusServiceUnavailable)
		return resultUnavailable
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return resultForwarded
}
