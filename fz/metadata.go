package fz

import "time"

const itemSize = 48

type Metadata struct {
	key    uint128
	offset int64
	size   uint64 // 16bit key length + 48bit size
	tstamp uint32
	crc32  uint32
	flag   uint64
}

func (m *Metadata) KeyLen() uint16 { return uint16(m.size >> 48) }

func (m *Metadata) BufLen() int64 { return int64(m.size & 0x0000ffffffffffff) }

func (m *Metadata) Flag() uint64 { return m.flag }

func (m *Metadata) Created() time.Time { return time.Unix(int64(m.tstamp), 0) }

func (m *Metadata) Crc32() uint32 { return m.crc32 }

func (m *Metadata) setKeyLen(ln uint16) {
	m.size &= 0x0000ffffffffffff
	m.size |= uint64(ln) << 48
}

func (m *Metadata) setBufLen(ln int64) {
	m.size &= 0xffff000000000000
	m.size |= uint64(ln) & 0x0000ffffffffffff
}
