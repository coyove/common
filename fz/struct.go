package fz

var (
	superBlockMagic = [4]byte{'z', 'z', 'z', '0'}
	jumpBlockMagic  = [4]byte{'y', 'y', 'y', '0'}
	blockMagic      = [4]byte{'x', 'x', 'x', '0'}
	nodeMagic       = [4]byte{'w', 'w', 'w', '0'}
)

type SuperBlock struct {
	magic     [4]byte
	reserved  [4]byte
	createdAt [4]byte
	blockSize [4]byte
	size      [8]byte
	count     [8]byte
	salt      [16]byte
}

type JumpBlock struct {
	table [256][4]byte
}

type Block struct {
	magic    [4]byte
	size     int8
	reserved [3]byte
	data     [504]byte
}

type Node struct {
}

func uint64To6Bytes(b []byte, v uint64) {
	b[0] = byte(v >> 40)
	b[1] = byte(v >> 32)
	b[2] = byte(v >> 24)
	b[3] = byte(v >> 16)
	b[4] = byte(v >> 8)
	b[5] = byte(v)
}

func uint64From6Bytes(b []byte) (v uint64) {
	return uint64(b[5]) | uint64(b[4])<<8 | uint64(b[3])<<16 | uint64(b[2])<<24 | uint64(b[1])<<32 | uint64(b[0])<<40
}
