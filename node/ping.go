package node

import (
	"fmt"
	"log"
	"time"
)

const heartbeat = time.Second

func init() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
}

func (n *Node) ping() {
	tick := time.NewTicker(heartbeat)

	ping := func(id string) error {
		resp, err := n.rpc(id, "ping")
		if err != nil {
			return err
		}
		if resp["id"] != id {
			return fmt.Errorf("ping failed: %s", id)
		}
		return nil
	}

TICK:
	for {
		select {
		case <-tick.C:
			n.isolated = n.quorum(ping, false) != nil
		case <-n.pingstop:
			break TICK
		}
	}

	tick.Stop()
}

func (n *Node) quorum(f func(id string) error, requireAllAcks bool) []error {
	pongs := make(chan [2]interface{}, len(n.allNodes)-1)

	for _, id := range n.allNodes {
		if id == n.ID {
			continue
		}
		go func(id string) {
			pongs <- [2]interface{}{id, f(id)}
		}(id)
	}

	var scores = 1
	var errs []error
	for {
		select {
		case res := <-pongs:
			if res[1] == nil {
				if scores++; scores >= len(n.allNodes)/2+1 && !requireAllAcks {
					return nil
				}
			} else {
				errs = append(errs, fmt.Errorf("quorum error of %v: %v", res[0], res[1]))
			}
		case <-time.After(heartbeat):
			return errs
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errs
}
