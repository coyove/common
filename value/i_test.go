package value

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestValues(t *testing.T) {
	rand.Seed(time.Now().Unix())

	for i := 0; i < 1e6; i++ {
		v := rand.Int63()
		v2, _ := Int64(v).Int64B()
		if v != v2 {
			t.Fatal(v, v2)
		}

		vs := strconv.FormatInt(v, 10)
		vs2, _ := String(vs).StringB()
		if vs2 != vs {
			t.Fatal(vs, vs2)
		}

		ii := Interface(vs).Value()
		if ii.(string) != vs {
			t.Fatal(vs, ii)
		}
	}
}
