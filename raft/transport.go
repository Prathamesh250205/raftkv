package raft

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Transport sends Raft RPCs to other nodes. An error means no reply was received.
type Transport interface {
	RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error
}

// RPCPath is where NewRPCHandler should be mounted. Raft traffic shares the
// node's HTTP port with the client API, so each node needs only one address.
const RPCPath = "/_raft/rpc"

const connectedStatus = "200 Connected to Go RPC"

var errRPCTimeout = errors.New("raft: rpc timeout")

type rpcService struct{ r *Raft }

func (s *rpcService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return s.r.HandleRequestVote(args, reply)
}

func (s *rpcService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return s.r.HandleAppendEntries(args, reply)
}

// NewRPCHandler serves r's RPC handlers over net/rpc (gob encoding, HTTP CONNECT).
func NewRPCHandler(r *Raft) (http.Handler, error) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &rpcService{r}); err != nil {
		return nil, err
	}
	return srv, nil
}

// HTTPTransport keeps one persistent RPC connection per peer and redials on failure.
type HTTPTransport struct {
	addrs   map[int]string
	timeout time.Duration

	mu      sync.Mutex
	clients map[int]*rpc.Client
}

func NewHTTPTransport(addrs map[int]string) *HTTPTransport {
	return &HTTPTransport{addrs: addrs, timeout: 200 * time.Millisecond, clients: map[int]*rpc.Client{}}
}

func (t *HTTPTransport) RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	return t.call(peer, "Raft.RequestVote", args, reply)
}

func (t *HTTPTransport) AppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return t.call(peer, "Raft.AppendEntries", args, reply)
}

func (t *HTTPTransport) call(peer int, method string, args, reply any) error {
	c, err := t.client(peer)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if errors.Is(call.Error, rpc.ErrShutdown) || errors.Is(call.Error, io.ErrUnexpectedEOF) {
			t.drop(peer, c)
		}
		return call.Error
	case <-time.After(t.timeout):
		t.drop(peer, c)
		return errRPCTimeout
	}
}

func (t *HTTPTransport) client(peer int) (*rpc.Client, error) {
	t.mu.Lock()
	c, ok := t.clients[peer]
	t.mu.Unlock()
	if ok {
		return c, nil
	}

	conn, err := net.DialTimeout("tcp", t.addrs[peer], t.timeout)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(t.timeout))
	io.WriteString(conn, "CONNECT "+RPCPath+" HTTP/1.0\n\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil || resp.Status != connectedStatus {
		conn.Close()
		if err == nil {
			err = errors.New("raft: unexpected handshake response " + resp.Status)
		}
		return nil, err
	}
	conn.SetDeadline(time.Time{})
	c = rpc.NewClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.clients[peer]; ok { // lost a race with another dial
		c.Close()
		return existing, nil
	}
	t.clients[peer] = c
	return c, nil
}

func (t *HTTPTransport) drop(peer int, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[peer] == c {
		delete(t.clients, peer)
	}
	t.mu.Unlock()
	c.Close()
}
