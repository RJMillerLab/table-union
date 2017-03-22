package search

import (
	"math"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
)

type VecEntry struct {
	TableID     int       `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Embedding   []float64 `json:"embedding"`
}

type SearchIndex struct {
	entries []*VecEntry
	tables  []*wikitable.WikiTable
}

func floatToInt(x float64) int {
	return int(math.Floor((x / math.MaxFloat64) * float64(math.MaxInt64)))
}

func dotProduct(x, y []float64) float64 {
	if len(x) != len(y) {
		panic("Length of vectors not equal")
	}
	p := 0.0
	for i := range x {
		p += x[i] * y[i]
	}
	return p
}

func (index *SearchIndex) TopK(query []float64, k int) []*VecEntry {
	queue := NewTopKQueue(k)
	for _, entry := range index.entries {
		queue.Push(entry, floatToInt(dotProduct(query, entry.Embedding)))
	}
	result := make([]*VecEntry, queue.Size())
	for i := len(result) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result[i] = v.(*VecEntry)
	}
	return result
}
