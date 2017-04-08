package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/benchmark"
	"github.com/RJMillerLab/table-union/wwt"
	"github.com/ekzhu/counter"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var wwtDir string
	var benchmarkSqliteDB string
	var negativeToPositiveRatio float64
	var fastTextSqliteDB string
	flag.StringVar(&wwtDir, "wwtdir", "/home/ekzhu/WWT/workspace/WWT_GroundTruth", "The top directory of the WWT benchmark xml files")
	flag.StringVar(&benchmarkSqliteDB, "bench-db", "", "The output SqliteDB file")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.Float64Var(&negativeToPositiveRatio, "neg2pos", 1.0, "The ratio of number of negative samples to positive ones")
	flag.Parse()
	if benchmarkSqliteDB == "" {
		panic("Missing output destination")
	}
	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)
	w := wwt.NewWWT(wwtDir, ft)

	// Read columns and group by annotations
	columns := make([]*wwt.WWTColumn, 0)
	groupByAnno := make(map[string][]int)
	var id int // the index of column
	for column := range w.ReadColumns() {
		columns = append(columns, column)
		// add column id to the annotation groups
		for _, anno := range column.Annotations {
			if _, exists := groupByAnno[anno]; !exists {
				groupByAnno[anno] = make([]int, 0)
			}
			groupByAnno[anno] = append(groupByAnno[anno], id)
		}
		id++
	}
	log.Printf("Read %d columns with embedding vectors", len(columns))

	// Produce positive pairs from the annotation groups
	positivePairs := make([]columnPair, 0)
	positivePairSeen := counter.NewCounter()
	for _, group := range groupByAnno {
		if len(group) == 1 {
			continue
		}
		// Get unique pairs in this group
		for p := range uniquePairs(group) {
			if positivePairSeen.Has(p.String()) {
				continue
			}
			positivePairs = append(positivePairs, p)
			positivePairSeen.Update(p.String())
		}
	}
	log.Printf("Found %d positive pairs", len(positivePairs))

	// Check total number of negative samples
	totalPairsCount := len(columns) * (len(columns) - 1) / 2
	negativePairsCount := int(negativeToPositiveRatio * float64(len(positivePairs)))
	if negativePairsCount > totalPairsCount {
		panic("Asking for too many negative samples")
	}

	// Generate negative samples
	negativePairs := make([]columnPair, 0)
	// Permutate to remove bias
	columnIDs := rand.Perm(len(columns))
	for p := range uniquePairs(columnIDs) {
		if len(negativePairs) >= negativePairsCount {
			break
		}
		// Skip positive pairs
		if positivePairSeen.Has(p.String()) {
			continue
		}
		negativePairs = append(negativePairs, p)
	}
	log.Printf("Created %d negative pairs", len(negativePairs))

	// Write all positive and negative pairs to SqliteDB
	out := make(chan *benchmark.SamplePair)
	go func() {
		for _, p := range positivePairs {
			col1 := columns[p.c1]
			col2 := columns[p.c2]
			out <- &benchmark.SamplePair{
				ID1:   col1.ColumnName,
				ID2:   col2.ColumnName,
				Vec1:  col1.Vec,
				Vec2:  col2.Vec,
				Label: 1,
			}
		}
		for _, p := range negativePairs {
			col1 := columns[p.c1]
			col2 := columns[p.c2]
			out <- &benchmark.SamplePair{
				ID1:   col1.ColumnName,
				ID2:   col2.ColumnName,
				Vec1:  col1.Vec,
				Vec2:  col2.Vec,
				Label: 0,
			}
		}
		close(out)
	}()
	benchmark.WriteToDB(out, benchmarkSqliteDB)
	log.Printf("Created labeled benchmark dataset %s", benchmarkSqliteDB)
}

type columnPair struct {
	c1 int
	c2 int
}

func newColumnPair(i, j int) columnPair {
	if i < j {
		return columnPair{i, j}
	}
	return columnPair{j, i}
}

func (p columnPair) String() string {
	return fmt.Sprintf("%d_%d", p.c1, p.c2)
}

func uniquePairs(columnIndexes []int) chan columnPair {
	if len(columnIndexes) < 2 {
		panic("Cannot find pairs when number of column indexes is less than 2")
	}
	out := make(chan columnPair)
	go func() {
		for i := 0; i < len(columnIndexes)-1; i++ {
			for j := i + 1; j < len(columnIndexes); j++ {
				out <- newColumnPair(columnIndexes[i], columnIndexes[j])
			}
		}
		close(out)
	}()
	return out
}
