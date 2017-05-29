package embserver

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/opendata"
)

const (
	LSHTableName = "lsh_index_sum"
	FastTextDim  = 300
)

var (
	ByteOrder = binary.BigEndian
)

type EmbEntry struct {
	TableID     string    `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	PCIndex     int       `json:"pc_index"`
	PCVec       []float64 `json:"vec"`
	PCVar       float64   `json:"var"`
	AvgVec      []float64 `json:"avgvec"`
	SumVec      []float64 `json:"sumvec""`
}

type LSHEntry struct {
	TableID     string
	ColumnIndex int
	BandIndex   int
	HashKey     uint64
}

type SearchIndex struct {
	lsh       *CosineLsh
	transFun  func(string) string
	tokenFun  func(string) []string
	domainDir string
	byteOrder binary.ByteOrder
}

func NewSearchIndex(domainDir string, lsh *CosineLsh) *SearchIndex {
	index := &SearchIndex{
		lsh:       lsh,
		transFun:  DefaultTransFun,
		tokenFun:  DefaultTokenFun,
		domainDir: domainDir,
		byteOrder: ByteOrder,
	}
	return index
}

func (index *SearchIndex) Build() error {
	domainfilenames := opendata.StreamFilenames()
	embfilenames := opendata.StreamEmbVectors(10, domainfilenames)
	count := 0
	for file := range embfilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Printf("file %s does not exist.", err)
			continue
		}
		count += 1
		if count%100 == 0 {
			log.Printf("indexed %d domains", count)
		}
		vec, err := embedding.ReadVecFromDisk(file, ByteOrder)
		if err != nil {
			return err
		}
		tableID, columnIndex := parseFilename(index.domainDir, file)
		index.lsh.Insert(vec, toColumnID(tableID, columnIndex))
	}
	return nil
}

func (index *SearchIndex) SaveLSHIndex(lshEntries <-chan *LSHEntry, lshdbFilename string) {
	// Open connection
	db, err := sql.Open("sqlite3", lshdbFilename)
	if err != nil {
		panic(err)
	}
	// Saving embedding entries to Sqlite
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id TEXT,
			column_index INTEGER,
			band_index INTEGER,
			hash_key INTEGER
		);
		`, LSHTableName))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(table_id, column_index, band_index, hash_key) VALUES(?, ?, ?, ?);
		`, LSHTableName))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for e := range lshEntries {
		if _, err := stmt.Exec(e.TableID, e.ColumnIndex, e.BandIndex, e.HashKey); err != nil {
			panic(err)
		}
	}
	_, err = db.Exec(fmt.Sprintf(`
		CREATE INDEX ind_band ON %s(band_index, hash_key);
		`, LSHTableName))
	if err != nil {
		panic(err)
	}
	db.Close()
}

func (index *SearchIndex) Get(tableID string, columnIndex int) (*EmbEntry, error) {
	vec, err := GetSumEmbVec(index.domainDir, tableID, columnIndex)
	if err != nil {
		panic(err)
	}
	return &EmbEntry{
		TableID:     tableID,
		ColumnIndex: columnIndex,
		SumVec:      vec,
	}, nil
}

func (index *SearchIndex) TopK(query []float64, k int) []*EmbEntry {
	start := time.Now()
	lshResults := index.lsh.Query(query)
	log.Printf("LSH Returns %d candidates in %.4f", len(lshResults), time.Now().Sub(start).Seconds())
	start = time.Now()
	queue := NewTopKQueue(k)
	for _, id := range lshResults {
		tableID, columnIndex := fromColumnID(id)
		entry, err := index.Get(tableID, columnIndex)
		if err != nil {
			panic(err)
		}
		queue.Push(entry, embedding.Cosine(query, entry.SumVec))
	}
	result := make([]*EmbEntry, queue.Size())
	for i := len(result) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result[i] = v.(*EmbEntry)
	}
	log.Printf("Post-proc took %.4f secs", time.Now().Sub(start).Seconds())
	return result
}
