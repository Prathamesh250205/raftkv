package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"raftkv/kv"
	"raftkv/raft"
)

func status(t *testing.T, method, url, token, body string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func TestAuthToken(t *testing.T) {
	c := newTestClusterWith(t, 3, 2, 0, Config{AuthToken: "s3cret", PublicReads: true})
	url := "http://" + c.addrs[1]

	if got := status(t, http.MethodPut, url+"/kv/k", "", "v"); got != http.StatusUnauthorized {
		t.Fatalf("write without token: %d, want 401", got)
	}
	if got := status(t, http.MethodPut, url+"/kv/k", "wrong", "v"); got != http.StatusUnauthorized {
		t.Fatalf("write with wrong token: %d, want 401", got)
	}
	if got := status(t, http.MethodDelete, url+"/shards/0/members/2", "", ""); got != http.StatusUnauthorized {
		t.Fatalf("membership change without token: %d, want 401", got)
	}
	c.put("k", "v") // the harness sends the right token
	if got := status(t, http.MethodGet, url+"/kv/k", "", ""); got != http.StatusOK {
		t.Fatalf("public read: %d, want 200", got)
	}
	if got := c.nodes[2].Raft(0).Status().Members; len(got) != 3 {
		t.Fatalf("an unauthorized request changed membership: %v", got)
	}
}

func TestPrivateReadsNeedToken(t *testing.T) {
	c := newTestClusterWith(t, 3, 1, 0, Config{AuthToken: "s3cret"})
	c.put("k", "v")
	if got := status(t, http.MethodGet, "http://"+c.addrs[1]+"/kv/k", "", ""); got != http.StatusUnauthorized {
		t.Fatalf("read without token: %d, want 401", got)
	}
}

func TestClusterSecretRejectsStrangers(t *testing.T) {
	c := newTestClusterWith(t, 3, 1, 0, Config{ClusterSecret: "cluster-key"})
	c.put("k", "v") // the members themselves still replicate

	for _, tc := range []struct {
		secret string
		ok     bool
	}{{"", false}, {"guess", false}, {"cluster-key", true}} {
		tr := raft.NewHTTPTransportPath(map[int]string{1: c.addrs[1]}, rpcPath(0))
		tr.SetSecret(tc.secret)
		var reply raft.RequestVoteReply
		err := tr.RequestVote(1, &raft.RequestVoteArgs{Term: 0, CandidateID: 99}, &reply)
		if (err == nil) != tc.ok {
			t.Fatalf("RPC with secret %q: err=%v, want success=%v", tc.secret, err, tc.ok)
		}
	}
}

func TestForwardModeAnswersDirectly(t *testing.T) {
	c := newTestClusterWith(t, 3, 2, 0, Config{Forward: true})
	noRedirects := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	// Pick a node that is NOT the leader for the key's shard.
	key := "forward-me"
	g := kv.ShardFor(key, c.shards)
	follower := -1
	for id, nd := range c.nodes {
		if nd.Raft(g).Status().State != "leader" {
			follower = id
			break
		}
	}
	url := "http://" + c.addrs[follower] + "/kv/" + key

	req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader("relayed"))
	resp, err := noRedirects.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT via follower in forward mode: %d, want 204 (not a redirect)", resp.StatusCode)
	}
	resp, err = noRedirects.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "relayed" {
		t.Fatalf("GET via follower: %d %q", resp.StatusCode, body)
	}
}
