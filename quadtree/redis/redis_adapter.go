package redis_adapter

import (
	"encoding/json"
	"strconv"

	"github.com/coyove/common/quadtree"
	"github.com/gomodule/redigo/redis"
)

var (
	HGetAll func(id string) (map[string][]byte, error)
	HSet    func(id, key string, v []byte) error
	HSetNX  func(id, key string, v []byte) (existed bool, err error)
	HDel    func(id, key string) error
	Del     func(id string) error
)

func init() {
	quadtree.Load = func(id string) (quadtree.QuadTree, error) {
		h, err := HGetAll(id)
		if h == nil {
			return quadtree.QuadTree{}, quadtree.ErrNotFound
		}
		if err != nil {
			return quadtree.QuadTree{}, err
		}
		t := quadtree.QuadTree{}
		if err := json.Unmarshal(h["t"], &t); err != nil {
			return quadtree.QuadTree{}, err
		}
		t.O[0], t.O[1], t.O[2], t.O[3] = string(h["0"]), string(h["1"]), string(h["2"]), string(h["3"])

		el, err := HGetAll(id + "elems")
		if err != nil {
			return quadtree.QuadTree{}, err
		}

		t.Elems = map[quadtree.Point]quadtree.Element{}
		for _, buf := range el {
			var e quadtree.Element
			if err := json.Unmarshal(buf, &e); err != nil {
				return quadtree.QuadTree{}, err
			}
			t.Elems[e.Point] = e
		}
		return t, nil
	}
	quadtree.Store = func(t quadtree.QuadTree) error {
		buf, _ := json.Marshal(t)
		return HSet(t.ID, "t", buf)
	}
	quadtree.StoreElement = func(id string, e quadtree.Element) error {
		buf, _ := json.Marshal(e)
		return HSet(id+"elems", e.Point.String(), buf)
	}
	quadtree.DeleteAllElements = func(id string) error {
		return Del(id + "elems")
	}
	quadtree.DeleteElement = func(id string, e quadtree.Element) error {
		return HDel(id+"elems", e.Point.String())
	}
	quadtree.StoreOrthant = func(id string, o int, oid string) (existed bool, err error) {
		return HSetNX(id, strconv.Itoa(o), []byte(oid))
	}
}

func SimpleInit(redisAddr string) {
	c := redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", redisAddr)
	}, 10)

	HGetAll = func(id string) (map[string][]byte, error) {
		c := c.Get()
		defer c.Close()
		m2, err := redis.StringMap(c.Do("HGETALL", id))
		if err != nil {
			return nil, err
		}
		m := make(map[string][]byte, len(m2))
		for k, v := range m2 {
			m[k] = []byte(v)
		}
		return m, nil
	}
	HDel = func(id, key string) error {
		c := c.Get()
		defer c.Close()
		_, err := redis.Bool(c.Do("HDEL", id, key))
		return err
	}
	Del = func(id string) error {
		c := c.Get()
		defer c.Close()
		_, err := redis.Bool(c.Do("DEL", id))
		return err
	}
	HSet = func(id, key string, v []byte) error {
		c := c.Get()
		defer c.Close()
		_, err := redis.Bool(c.Do("HSET", id, key, v))
		return err
	}
	HSetNX = func(id, key string, v []byte) (bool, error) {
		c := c.Get()
		defer c.Close()
		r, err := redis.Int(c.Do("HSETNX", id, key, v))
		return r == 0, err
	}
}
