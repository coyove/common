package node

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type slot struct {
	v   string
	gen int64
	d   int64
}

func (s *slot) String() string {
	return fmt.Sprintf("%s(gen:%d)", strconv.Quote(s.v), s.gen)
}

type Node struct {
	ID       string
	Values   sync.Map
	stopmu   sync.Mutex
	allNodes []string
	locks    *KeyLocks
	server   *http.Server
	pingstop chan bool
	isolated bool
	stopped  bool
}

func NewNode(id string, allNodes ...string) (*Node, error) {
	if len(allNodes) < 2 {
		return nil, fmt.Errorf("at least 2 nodes are required")
	}

	n := &Node{
		ID:       id,
		locks:    NewKeyLocks(),
		allNodes: allNodes,
		pingstop: make(chan bool),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", n.wrapRPCHandler(n.handlePing))
	mux.HandleFunc("/get", n.wrapRPCHandler(n.handleGet))
	mux.HandleFunc("/lock", n.wrapRPCHandler(n.handleLock))
	mux.HandleFunc("/commit_unlock", n.wrapRPCHandler(n.handleCommitUnlock))
	mux.HandleFunc("/unlock", n.wrapRPCHandler(n.handleUnlock))

	n.server = &http.Server{Addr: n.ID}
	n.server.Handler = mux

	go n.ping()

	err := make(chan error, 1)
	go func() {
		e := n.server.ListenAndServe()
		err <- e
		if e != nil && !n.stopped {
			log.Println(n.ID, "listen error:", e)
			n.Stop()
		}
	}()

	select {
	case e := <-err:
		return nil, e
	case <-time.After(time.Millisecond * 150):
		// Wait a short time to catch any immediate errors of listening (e.g. port already listened)
	}

	return n, nil
}

func (n *Node) Stop() error {
	n.stopmu.Lock()
	defer n.stopmu.Unlock()
	if n.stopped {
		return fmt.Errorf("close a closed node")
	}

	n.stopped = true
	n.pingstop <- true
	n.server.Shutdown(context.TODO())
	return n.server.Close()
}

func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Write(simpleJSON("id", n.ID))
}

func (n *Node) handleLock(w http.ResponseWriter, r *http.Request) {
	k := r.FormValue("k")

	// Key with valid value can't be locked, unless we want to delete it
	if _, ok := n.Values.Load(k); ok && !strings.HasPrefix(uuid, "delete") {
		w.Write(simpleJSON("status", "key-has-value"))
		return
	}

	if n.locks.Lock(k, r.FormValue("uuid"), heartbeat*2) {
		w.Write(simpleJSON("status", "ok", "gen", i.(*slot).gen+1))
	} else {
		w.Write(simpleJSON("status", "already-locked"))
	}
}

func (n *Node) handleCommitUnlock(w http.ResponseWriter, r *http.Request) {
	k, uuid, v := r.FormValue("k"), r.FormValue("uuid"), r.FormValue("v")

	if !n.locks.IsLocked(k, uuid) {
		w.Write(simpleJSON("status", "invalid-lock-state"))
		return
	}

	if strings.HasPrefix(uuid, "delete") {
		n.Values.Delete(k)
	} else {
		if _, ok := n.Values.Load(); ok {
			w.Write(simpleJSON("status", "key-has-value"))
			return
		}
		n.Values.Store(k, v)
	}

	n.locks.Unlock(k, uuid)
	w.Write(simpleJSON("status", "ok"))
}

func (n *Node) handleUnlock(w http.ResponseWriter, r *http.Request) {
	n.locks.Unlock(r.FormValue("k"), r.FormValue("uuid"))
	w.Write(simpleJSON("status", "ok"))
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	if n.isolated {
		w.Write(simpleJSON("status", "isolation"))
		return
	}

	v, ok := n.Values.Load(r.FormValue("k"))

	if !ok {
		w.Write(simpleJSON("status", "ok", "value", ""))
	} else {
		w.Write(simpleJSON("status", "ok", "value", v))
	}
}
