package minhashlsh

import (
	"strconv"
	"testing"
)

func Benchmark_MinhashLSH_Insert10000(b *testing.B) {
	sigs := make([]Signature, 10000)
	for i := range sigs {
		sigs[i] = randomSignature(64, int64(i))
	}
	b.ResetTimer()
	f := NewMinhashLSH16(64, 0.5)
	for i := range sigs {
		f.Add(strconv.Itoa(i), sigs[i])
	}
	f.Index()
}
