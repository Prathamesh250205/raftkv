package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"raftkv/kv"
)

func call(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// eventually retries f until it returns true, for up to 10 seconds.
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if f() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDemoSurvivesCrash(t *testing.T) {
	d, err := newDemo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.close)
	srv := httptest.NewServer(d.handler())
	t.Cleanup(srv.Close)

	if code, body := call(t, http.MethodGet, srv.URL+"/", ""); code != 200 || !strings.Contains(body, "raftkv") {
		t.Fatalf("dashboard page: %d", code)
	}
	eventually(t, "a first write", func() bool {
		code, _ := call(t, http.MethodPut, srv.URL+"/kv/greeting", "survives")
		return code == http.StatusNoContent
	})

	// Crash whichever node leads the key's shard.
	g := kv.ShardFor("greeting", shardCount)
	var leader int
	eventually(t, "a leader for the key's shard", func() bool {
		for _, n := range d.state().Nodes {
			if n.Up && n.Shards[g].State == "leader" {
				leader = n.ID
				return true
			}
		}
		return false
	})
	if code, msg := call(t, http.MethodPost, srv.URL+"/api/crash/"+strconv.Itoa(leader), ""); code != http.StatusNoContent {
		t.Fatalf("crash node %d: %d %s", leader, code, msg)
	}

	// A second crash while one node is down must be refused: it would lose the majority.
	other := leader%nodeCount + 1
	if code, _ := call(t, http.MethodPost, srv.URL+"/api/crash/"+strconv.Itoa(other), ""); code != http.StatusConflict {
		t.Fatalf("second concurrent crash: got %d, want 409", code)
	}

	// The committed value survives, served by the new leader.
	eventually(t, "the value after the leader crashed", func() bool {
		code, body := call(t, http.MethodGet, srv.URL+"/kv/greeting", "")
		return code == http.StatusOK && body == "survives"
	})

	// The crashed node comes back, and the event log tells the story.
	eventually(t, "the crashed node to restart", func() bool {
		for _, n := range d.state().Nodes {
			if n.ID == leader {
				return n.Up
			}
		}
		return false
	})
	var st stateView
	_, body := call(t, http.MethodGet, srv.URL+"/api/state", "")
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	var log []string
	for _, e := range st.Events {
		log = append(log, e.Text)
	}
	joined := strings.Join(log, "\n")
	for _, want := range []string{"crashed", "won the election", "restarted"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("event log is missing %q:\n%s", want, joined)
		}
	}
}

func TestDemoLimitsInput(t *testing.T) {
	d, err := newDemo(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.close)
	srv := httptest.NewServer(d.handler())
	t.Cleanup(srv.Close)

	if code, _ := call(t, http.MethodGet, srv.URL+"/kv/"+strings.Repeat("k", maxKeyBytes+1), ""); code != http.StatusBadRequest {
		t.Fatalf("over-long key: got %d, want 400", code)
	}
	eventually(t, "the cluster to accept writes", func() bool {
		code, _ := call(t, http.MethodPut, srv.URL+"/kv/small", "ok")
		return code == http.StatusNoContent
	})
	if code, _ := call(t, http.MethodPut, srv.URL+"/kv/big", strings.Repeat("v", maxValueBytes+1)); code == http.StatusNoContent {
		t.Fatal("an over-size value was accepted")
	}
}
