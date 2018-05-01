package session

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/coyove/common/rand"
)

var repository struct {
	sync.Mutex
	rand      *rand.Rand
	blk       cipher.Block
	oldTokens map[[16]byte]bool
}

const (
	TTL = 86400
)

func init() {
	repository.rand = rand.New()
	iv := repository.rand.Fetch(16)
	repository.blk, _ = aes.NewCipher(iv)
	repository.oldTokens = make(map[[16]byte]bool)
}

// New returns a token for the session
func New(extra string) (tok [16]byte) {
	ts := uint32(time.Now().Unix())
	binary.LittleEndian.PutUint32(tok[:4], ts)
	copy(tok[4:8], extra)
	x := sha1.Sum(tok[:8])
	copy(tok[8:], x[:])
	repository.blk.Encrypt(tok[:], tok[:])

	if repository.rand.Intn(1024) == 0 {
		go func() {
			repository.Lock()
			now := uint32(time.Now().Unix())
			for tok := range repository.oldTokens {
				ts := binary.LittleEndian.Uint32(tok[:4])
				if ts > now || now-ts > TTL {
					delete(repository.oldTokens, tok)
				}
			}
			repository.Unlock()
		}()
	}

	return
}

// NewString returns a string token for the session
func NewString(extra string) string {
	return fmt.Sprintf("%x", New(extra))
}

// Consume validates the token and consumes it (if true)
func Consume(tok [16]byte, extra string) bool {
	repository.blk.Decrypt(tok[:], tok[:])
	x := sha1.Sum(tok[:8])
	if !bytes.Equal(x[:8], tok[8:]) {
		return false
	}

	if string(tok[4:8]) != extra[:4] {
		return false
	}

	now := uint32(time.Now().Unix())
	ts := binary.LittleEndian.Uint32(tok[:4])

	if now < ts {
		return false
	}
	if now-ts > TTL {
		return false
	}

	repository.Lock()
	if repository.oldTokens[tok] {
		repository.Unlock()
		return false
	}
	repository.oldTokens[tok] = true
	repository.Unlock()
	return true
}

// ConsumeString validates the token and consumes it (if true)
func ConsumeString(tok string, extra string) bool {
	if len(tok) != 32 {
		return false
	}

	var t [16]byte
	for i := 0; i < 16; i++ {
		n, err := strconv.ParseInt(tok[i*2:i*2+2], 16, 64)
		if err != nil {
			return false
		}
		t[i] = byte(n)
	}

	return Consume(t, extra)
}
