package node

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.com/ra_raven/go-common/rand"
)

var (
	ErrTemporaryFailure = fmt.Errorf("temporary failure")
	ErrKeyNotFound      = fmt.Errorf("key not found")
	ErrIsolatedNode     = fmt.Errorf("isolated node")
	ErrStoppedNode      = fmt.Errorf("stopped node")

	magicDeleteValue = randomString("z")
)

func randomString(k string) string {
	x := sha1.Sum([]byte(strconv.FormatInt(rand.Int63()^time.Now().UnixNano(), 10) + k))
	return hex.EncodeToString(x[:16])
}

func collapseErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	tmpstr := ErrTemporaryFailure.Error()
	g := bytes.Buffer{}
	g.WriteString(fmt.Sprintf("total %d errors:", len(errs)))

	for i, err := range errs {
		if strings.Contains(err.Error(), tmpstr) {
			return ErrTemporaryFailure
		}

		g.WriteString(fmt.Sprintf(" %d:", i))
		g.WriteString(err.Error())
	}
	return errors.New(g.String())
}

func (n *Node) String() string {
	p := bytes.Buffer{}
	p.WriteString(fmt.Sprintf("\n%s (%v)\n", n.ID, n.isolated))
	n.Values.Range(func(k, v interface{}) bool {
		p.WriteString(fmt.Sprintf("\t%v = %v\n", k, v))
		return true
	})
	return p.String()
}

func (n *Node) Delete(k string) error {
	_, err := n.Put(k, magicDeleteValue)
	return err
}

func (n *Node) Get(k string) (string, error) {
	switch {
	case n.isolated:
		return "", ErrIsolatedNode
	case n.stopped:
		return "", ErrStoppedNode
	}

	if v, ok := n.Values.Load(k); ok {
		return v, nil
	}

	wg := sync.WaitGroup{}
	scores := map[string]int{"": 1}

	for _, id := range n.allNodes {
		if id == n.ID {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			resp, err := n.rpc(id, "get", "k", k)
			if err == nil && resp["status"] == "ok" {
				scores[resp["value"]]++
			}
		}(id)
	}

	wg.Wait()
	for v, s := range scores {
		if s >= len(n.allNodes)/2+1 {
			if v != "" {
				n.Values.Store(k, v)
			}
			return v, nil
		}
	}
	return "", ErrTemporaryFailure
}

func (n *Node) Put(k string, v string) error {
	switch {
	case n.isolated:
		return ErrIsolatedNode
	case n.stopped:
		return ErrStoppedNode
	}

	uuid := randomString(k)
	if v == magicDeleteValue {
		uuid = "delete" + uuid
	}

	n.locks.MustLock(k, uuid, heartbeat)
	defer n.locks.Unlock(k, uuid)

	if _, ok := n.Values.Load(k); ok && v != magicDeleteValue {
		return fmt.Errorf("%s already had a value", k)
	}

	errs := n.quorum(func(id string) error {
		resp, err := n.rpc(id, "lock", "k", k, "uuid", uuid)
		if err != nil {
			return err
		}
		if resp["status"] != "ok" {
			return ErrTemporaryFailure
		}
		return nil
	}, false)

	if len(errs) > 0 {
		// Failed to lock, so unlock all
		n.quorum(func(id string) error {
			if _, err := n.rpc(id, "unlock", "k", k, "uuid", uuid); err != nil {
				// log.Println(n.ID, "unlock error:", k, err)
				return err
			}
			return nil
		}, false)
		return collapseErrors(errs)
	}

	// If we want to delete a value, we must receive acks from all nodes
	if errs = n.quorum(func(id string) error {
		resp, err := n.rpc(id, "commit_unlock", "k", k, "uuid", uuid, "v", v, "gen", maxgen)
		if err != nil {
			return err
		}
		if resp["status"] != "ok" {
			return fmt.Errorf("unexpected commit result: %v", resp)
		}
		return nil
	}, v == magicDeleteValue); len(errs) > 0 {
		log.Println("commit_unlock:", collapseErrors(errs))
		// If "lock" succeeded but "commit_unlock" failed, this node lost something
		// Here we treat it as being isolated
		return ErrIsolatedNode
	}

	if v == magicDeleteValue {
		n.Values.Delete(k)
	} else {
		n.Values.Store(k, v)
	}
	return nil
}
