package redis_adapter

import (
	"bytes"
	"encoding/gob"
	"strconv"

	"github.com/coyove/common/quadtree"
	"github.com/gomodule/redigo/redis"
)

type Implement struct {
	HGetAll func(id string) (map[string][]byte, error)
	HSet    func(id, key string, v []byte) error
	HSetNX  func(id, key string, v []byte) (existed bool, err error)
	HDel    func(id, key string) error
	Del     func(id string) error
}

type Database struct {
	i Implement
}

func (db *Database) Load(id string) (quadtree.QuadTree, error) {
	h, err := db.i.HGetAll(id)
	if err != nil {
		return quadtree.QuadTree{}, err
	}
	t := quadtree.QuadTree{}
	if err := gob.NewDecoder(bytes.NewReader(h["t"])).Decode(&t); err != nil {
		return quadtree.QuadTree{}, err
	}
	t.O[0], t.O[1], t.O[2], t.O[3] = string(h["0"]), string(h["1"]), string(h["2"]), string(h["3"])

	el, err := db.i.HGetAll(id + "elems")
	if err != nil {
		return quadtree.QuadTree{}, err
	}

	t.Elems = map[quadtree.Point]quadtree.Element{}
	for _, buf := range el {
		var e quadtree.Element
		if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&e); err != nil {
			return quadtree.QuadTree{}, err
		}
		t.Elems[e.Point] = e
	}
	return t, nil
}

func (db *Database) Store(t quadtree.QuadTree) error {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(t)
	return db.i.HSet(t.ID, "t", buf.Bytes())
}

func (db *Database) StoreElement(id string, e quadtree.Element) error {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(e)
	return db.i.HSet(id+"elems", e.Point.Marshal(), buf.Bytes())
}

func (db *Database) DeleteAllElements(id string) error {
	return db.i.Del(id + "elems")
}

func (db *Database) DeleteElement(id string, e quadtree.Element) error {
	return db.i.HDel(id+"elems", e.Point.Marshal())
}

func (db *Database) StoreOrthant(id string, o int, oid string) (existed bool, err error) {
	return db.i.HSetNX(id, strconv.Itoa(o), []byte(oid))
}

func New(redisInstance interface{}) *Database {
	i, ok := redisInstance.(Implement)
	if ok {
		return &Database{i: i}
	}
	c, ok := redisInstance.(*redis.Pool)
	if !ok {
		c = redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial("tcp", redisInstance.(string))
		}, 10)
	}
	return &Database{
		i: Implement{
			HGetAll: func(id string) (map[string][]byte, error) {
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
			},
			HDel: func(id, key string) error {
				c := c.Get()
				defer c.Close()
				_, err := redis.Bool(c.Do("HDEL", id, key))
				return err
			},
			Del: func(id string) error {
				c := c.Get()
				defer c.Close()
				_, err := redis.Bool(c.Do("DEL", id))
				return err
			},
			HSet: func(id, key string, v []byte) error {
				c := c.Get()
				defer c.Close()
				_, err := redis.Bool(c.Do("HSET", id, key, v))
				return err
			},
			HSetNX: func(id, key string, v []byte) (bool, error) {
				c := c.Get()
				defer c.Close()
				r, err := redis.Int(c.Do("HSETNX", id, key, v))
				return r == 0, err
			},
		},
	}
}
