package search

import (
	"log"
	"math"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
)

type VecEntry struct {
	TableID     int       `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Embedding   []float64 `json:"embedding"`
}

type SearchIndex struct {
	Entries []*VecEntry
	Tables  []*wikitable.WikiTable
}

func floatToInt(x float64) int {
	//return int(math.Floor((x / math.MaxFloat64) * float64(math.MaxInt64)))
	log.Printf("dp is %d\n", int(math.Floor((x/math.MaxFloat64)*float64(math.MaxInt64))))
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

func (index *SearchIndex) TopK(query []float64, k int) ([]*VecEntry, [][]string) {
	queue := NewTopKQueue(k)
	for _, entry := range index.Entries {
		queue.Push(entry, floatToInt(dotProduct(query, entry.Embedding)))
	}
	result_emvecs := make([]*VecEntry, queue.Size())
	result_columns := make([][]string, queue.Size())
	for i := len(result_emvecs) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result_emvecs[i] = v.(*VecEntry)
	}
	for i, v := range result_emvecs {
		for _, t := range index.Tables {
			if v.TableID == t.ID {
				result_columns[i] = t.Columns[v.ColumnIndex]
			}
		}
	}
	return result_emvecs, result_columns
}
