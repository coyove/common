package node

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())
}

func TestPing(t *testing.T) {
	var ids = []string{":20000", ":20001", ":20002"}
	nodes := [3]*Node{}
	for i := range nodes {
		nodes[i], _ = NewNode(ids[i], ids...)
	}

	time.Sleep(time.Second * 5)

	for _, n := range nodes {
		n.Stop()
	}
}

func printAllNodes(nodes []*Node) {
	fmt.Println("======== print ========")
	for _, n := range nodes {
		fmt.Println(n.String())
	}
	fmt.Println("======== print ========")
}

func getFromAllNodes(nodes []*Node, key string) (string, int64) {
	maxv, maxgen := "", int64(0)
	for _, n := range nodes {
		v, gen, _ := n.Get(key)
		if gen > maxgen {
			maxv = v
			maxgen = gen
		}
	}
	return maxv, maxgen
}

func TestPut(t *testing.T) {
	ids := []string{":20000", ":20001", ":20002"}
	nodes := make([]*Node, len(ids))
	for i := range nodes {
		nodes[i], _ = NewNode(ids[i], ids...)
	}

	go func() {
		time.Sleep(time.Second * 5)
		nodes[1].Stop()
		log.Println("#### Node 1 stopped")
		time.Sleep(time.Second * 10)
		nodes[1], _ = NewNode(ids[1], ids...)
		log.Println("#### Node 1 started")
	}()

	for i := 0; i < 10; i++ {
		wg := sync.WaitGroup{}
		for k := 0; k < 10; k++ {
			wg.Add(1)
			go func(k int) {
			RETRY:
				if _, err := nodes[rand.Intn(len(nodes))].Put("zzz", strconv.Itoa(i*100+k)); err != nil {
					if err != ErrTemporaryFailure {
						log.Println("TestPutSingleWrite:", err)
					}
					goto RETRY
				}
				//		printAllNodes(nodes)
				wg.Done()
			}(k)
		}
		wg.Wait()
	}

	for _, n := range nodes {
		n.Stop()
	}
}

func TestPutDead(t *testing.T) {
	ids := []string{":20000", ":20001", ":20002"}
	nodes := make([]*Node, len(ids))
	for i := range nodes {
		nodes[i], _ = NewNode(ids[i], ids...)
	}

	nodes[1].Stop()
	nodes[0].Put("zz", "z")
	nodes[1], _ = NewNode(ids[1], ids...)
	time.Sleep(time.Second)
	printAllNodes(nodes)

	log.Println(nodes[1].Get("zz"))
	printAllNodes(nodes)
	for _, n := range nodes {
		n.Stop()
	}
}

func TestFuzzy(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ids := []string{}
	keys := []string{}
	for i := 0; i < 10; i++ {
		ids = append(ids, fmt.Sprintf(":%d", 20000+i))
	}

	for i := 0; i < 100; i++ {
		keys = append(keys, "key"+randomString("")[:4])
	}

	nodes := make([]*Node, len(ids))
	for i := range nodes {
		nodes[i], _ = NewNode(ids[i], ids...)
	}

	start := time.Now()
	running := time.Second * 30
	stop, currentDead := false, 0
	go func() {
		for {
			time.Sleep(time.Second)
			if stop {
				return
			}
			if rand.Intn(3) == 0 {
				if currentDead < len(ids)/2-1 {
					for _, n := range nodes {
						if !n.stopped {
							log.Println(n.ID, "is dying", n.Stop())
							currentDead++
							break
						}
					}
				}
			}
			if rand.Intn(3) == 0 && time.Now().Sub(start) < running-time.Second {
				if currentDead > 0 {
					for i, n := range nodes {
						if n.stopped {
							log.Println(n.ID, "is starting")
							nodes[i], _ = NewNode(ids[i], ids...)
							currentDead--
							break
						}
					}
				}
			}
			if rand.Intn(3) == 0 {
				parts := strings.Split(debug_stop_traffic_between, ",")
				if len(parts) < len(ids)/2 {
					debug_stop_traffic_between += "," + ids[rand.Intn(len(ids))] + "," + ids[rand.Intn(len(ids))]
				}
				log.Println("network down:", debug_stop_traffic_between)
			}
			if rand.Intn(3) == 0 && time.Now().Sub(start) < running-time.Second {
				parts := strings.Split(debug_stop_traffic_between, ",")
				if len(parts) > 1 {
					parts = parts[:len(parts)-2]
				}
				debug_stop_traffic_between = strings.Join(parts, ",")
				log.Println("network down:", debug_stop_traffic_between)
			}
		}
	}()

	results := sync.Map{}
	for out := start.Add(running); time.Now().Before(out); {
		wg := sync.WaitGroup{}
		for k := 0; k < 10; k++ {
			wg.Add(1)
			go func() {
				k := keys[rand.Intn(len(keys))]
				v := randomString(k)[:8]
			RETRY:
				if _, err := nodes[rand.Intn(len(nodes))].Put(k, v); err != nil {
					if err != ErrTemporaryFailure {
						log.Println("Fuzzy:", err)
					}
					goto RETRY
				}

				results.Store(k, v)
				wg.Done()
			}()
		}
		wg.Wait()
	}

	stop = true

	for i, n := range nodes {
		if n.stopped {
			var err error
			nodes[i], err = NewNode(ids[i], ids...)
			log.Println("start", ids[i], err)
		}
	}

	results.Range(func(k, v interface{}) bool {
		v2, gen2, _ := nodes[rand.Intn(len(nodes))].Get(k.(string))
		all := ""
		if v != v2 {
			v2, gen2 = getFromAllNodes(nodes, k.(string))
			all = "(retried)"
		}
		if v == v2 {
			log.Println(k, "=", v, "(", gen2, ")", all)
		} else {
			log.Println(k, "=", v, "get:", v2, gen2, all)
		}
		return true
	})

	for _, n := range nodes {
		n.Stop()
	}
}

func TestLocalDriver(t *testing.T) {
	d := LocalDriver{kv: map[string][]byte{}}
	d.Push("a")
	d.Push("b")
	t.Log(d.Pop())
	t.Log(d.Pop())
	t.Log(d.Pop())
}
