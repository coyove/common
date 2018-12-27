package fz

import "strconv"

type uint128 [2]uint64

func (l uint128) less(r uint128) bool {
	if l[0] == r[0] {
		return l[1] < r[1]
	}
	return l[0] < r[0]
}

const (
	offset128Lower  = 0x62b821756295c58d
	offset128Higher = 0x6c62272e07bb0142
	prime128Lower   = 0x13b
	prime128Shift   = 24
)

func hashString(str string) uint128 {
	var s uint128
	s[0] = offset128Higher
	s[1] = offset128Lower

	for _, c := range str {
		s[1] ^= uint64(c)
		// Compute the multiplication in 4 parts to simplify carrying
		s1l := (s[1] & 0xffffffff) * prime128Lower
		s1h := (s[1] >> 32) * prime128Lower
		s0l := (s[0]&0xffffffff)*prime128Lower + (s[1]&0xffffffff)<<prime128Shift
		s0h := (s[0]>>32)*prime128Lower + (s[1]>>32)<<prime128Shift
		// Carries
		s1h += s1l >> 32
		s0l += s1h >> 32
		s0h += s0l >> 32
		// Update the values
		s[1] = (s1l & 0xffffffff) + (s1h << 32)
		s[0] = (s0l & 0xffffffff) + (s0h << 32)
	}

	if false {
		x, _ := strconv.Atoi(str)
		s[0] = uint64(x) >> 3
		s[1] = uint64(x)
	}

	return s
}
