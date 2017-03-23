package search

import (
	"errors"
	"io"
	"log"
	"strings"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	ErrNoEmbFound = errors.New("No embedding found")
)

type VecEntry struct {
	TableID     int       `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Embedding   []float64 `json:"embedding"`
}

type SearchIndex struct {
	ft       *fasttext.FastText
	entries  []*VecEntry
	tables   []*wikitable.WikiTable
	transFun func(string) string
}

func NewSearchIndex(ft *fasttext.FastText) *SearchIndex {
	return &SearchIndex{
		ft:       ft,
		entries:  make([]*VecEntry, 0),
		tables:   make([]*wikitable.WikiTable, 0),
		transFun: func(s string) string { return strings.TrimSpace(strings.ToLower(s)) },
	}
}

func (index *SearchIndex) Build(wikiTableFile io.Reader) error {
	for table := range wikitable.ReadWikiTable(wikiTableFile) {
		for i, column := range table.Columns {
			vec, err := index.GetEmb(column)
			if err == ErrNoEmbFound {
				log.Printf("No embedding found for table %d column %d", table.ID, i)
				continue
			}
			index.entries = append(index.entries, &VecEntry{
				TableID:     table.ID,
				ColumnIndex: i,
				Embedding:   vec,
			})
		}
		index.tables = append(index.tables, table)
	}
	return nil
}

func (index *SearchIndex) GetTable(id int) *wikitable.WikiTable {
	return index.tables[id]
}

func (index *SearchIndex) TopK(query []float64, k int) []*VecEntry {
	queue := NewTopKQueue(k)
	for _, entry := range index.entries {
		queue.Push(entry, dotProduct(query, entry.Embedding))
	}
	result := make([]*VecEntry, queue.Size())
	for i := len(result) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result[i] = v.(*VecEntry)
	}
	return result
}

func (index *SearchIndex) GetEmb(column []string) ([]float64, error) {
	domain := mkDomain(column, index.transFun)
	var vec []float64
	for w := range domain {
		wordparts := strings.Split(w, " ")
		for _, p := range wordparts {
			emb, err := index.ft.GetEmb(p)
			if err == fasttext.ErrNoEmbFound {
				log.Printf("No embedding found for %s", p)
				continue
			}
			if err != nil {
				panic(err)
			}
			if vec == nil {
				vec = emb.Vec
			} else {
				add(vec, emb.Vec)
			}
		}
	}
	if vec == nil {
		return nil, ErrNoEmbFound
	}
	return vec, nil
}

func mkDomain(values []string, transFun func(string) string) map[string]bool {
	domain := make(map[string]bool)
	for _, v := range values {
		v = transFun(v)
		domain[v] = true
	}
	return domain
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

func add(dst, src []float64) {
	if len(dst) != len(src) {
		panic("Length of vectors not equal")
	}
	for i := range src {
		dst[i] = dst[i] + src[i]
	}
}
