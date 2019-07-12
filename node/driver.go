package node

import (
	"encoding/binary"
	"fmt"
	"sync"
)

type Driver interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	Push(key string) error
	Pop() (string, error)
}

type LocalDriver struct {
	mu sync.Mutex
	kv map[string][]byte
}

func (d LocalDriver) Get(key string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.kv[key], nil
}

func (d LocalDriver) Delete(key string) error {
	d.mu.Lock()
	delete(d.kv, key)
	d.mu.Unlock()
	return nil
}

func (d LocalDriver) Set(key string, value []byte) error {
	d.mu.Lock()
	d.kv[key] = value
	d.mu.Unlock()
	return nil
}

func (d LocalDriver) Push(key string) error {
	buf, err := d.Get("q-counter")
	if err != nil {
		return err
	}

	var qCounter uint64
	if len(buf) == 8 {
		qCounter = binary.BigEndian.Uint64(buf) + 1
	} else {
		buf = make([]byte, 8)
		qCounter = 1
	}

	if err := d.Set(fmt.Sprintf("q-%d", qCounter), []byte(key)); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(buf, qCounter)
	return d.Set("q-counter", buf)
}

func (d LocalDriver) Pop() (string, error) {
	buf, err := d.Get("q-counter")
	if err != nil {
		return "", err
	}
	if len(buf) != 8 {
		return "", fmt.Errorf("invalid q-counter")
	}

	qCounter := binary.BigEndian.Uint64(buf)
	if qCounter == 0 {
		return "", nil
	}

	keybuf, err := d.Get(fmt.Sprintf("q-%d", qCounter))
	if err != nil {
		return "", err
	}

	binary.BigEndian.PutUint64(buf, qCounter-1)
	if err := d.Set("q-counter", buf); err != nil {
		return "", err
	}

	return string(keybuf), nil
}
