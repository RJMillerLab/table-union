package embserver

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

const (
	TableName    = "search_index"
	LSHTableName = "lsh_index"
)

var (
	ByteOrder = binary.BigEndian
)

type EmbEntry struct {
	TableID     string    `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Vec         []float64 `json:"vec"`
}

type LSHEntry struct {
	TableID     string
	ColumnIndex int
	BandIndex   int
	HashKey     uint64
}

type SearchIndex struct {
	ft        *fasttext.FastText
	db        *sql.DB // Sqlite store for the embedding entries
	lsh       *CosineLsh
	transFun  func(string) string
	tokenFun  func(string) []string
	tablename string
	byteOrder binary.ByteOrder
}

func NewSearchIndex(ft *fasttext.FastText, dbFilename string, lsh *CosineLsh) *SearchIndex {
	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		panic(err)
	}
	index := &SearchIndex{
		ft:        ft,
		db:        db,
		lsh:       lsh,
		transFun:  DefaultTransFun,
		tokenFun:  DefaultTokenFun,
		tablename: TableName,
		byteOrder: ByteOrder,
	}
	return index
}

func (index *SearchIndex) Close() error {
	return index.db.Close()
}

func (index *SearchIndex) IsNotBuilt() bool {
	return !index.checkSqlitTableExist()
}

func (index *SearchIndex) Build(ts *wikitable.WikiTableStore) error {
	if index.checkSqlitTableExist() {
		return errors.New("Sqlite database already exists")
	}
	if ts.IsNotBuilt() {
		return errors.New("Wikitable store is not built, build it first")
	}

	// Compute embedding entries from wikitables
	entries := make(chan *EmbEntry)
	go func() {
		defer close(entries)
		ts.Apply(func(table *wikitable.WikiTable) {
			for i, column := range table.Columns {
				if table.Headers[i].IsNum {
					continue
				}
				vec, err := embedding.GetDomainEmbPCA(index.ft, index.tokenFun, index.transFun, column)
				if err != nil {
					continue
				}
				id := toColumnID(table.ID, i)
				index.lsh.Insert(vec, id)
				entry := &EmbEntry{
					TableID:     table.ID,
					ColumnIndex: i,
					Vec:         vec,
				}
				entries <- entry
			}
		})
	}()

	// Saving embedding entries to Sqlite
	_, err := index.db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id TEXT,
			column_index INTEGER,
			vec BLOB
		);
		`, index.tablename))
	if err != nil {
		panic(err)
	}
	stmt, err := index.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(table_id, column_index, vec) VALUES(?, ?, ?);
		`, index.tablename))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for e := range entries {
		binVec := embedding.VecToBytes(e.Vec, index.byteOrder)
		if _, err := stmt.Exec(e.TableID, e.ColumnIndex, binVec); err != nil {
			panic(err)
		}
	}
	_, err = index.db.Exec(fmt.Sprintf(`
		CREATE INDEX ind_column_id ON %s(table_id, column_index);
		`, index.tablename))
	if err != nil {
		panic(err)
	}

	return nil
}

// Load recovers the search index entries from the Sqlite database.
func (index *SearchIndex) Load() {
	rows, err := index.db.Query(fmt.Sprintf(`
	SELECT table_id, column_index, vec FROM %s;
	`, index.tablename))
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var tableID string
		var columnIndex int
		var binVec []byte
		if err := rows.Scan(&tableID, &columnIndex, &binVec); err != nil {
			panic(err)
		}
		vec, err := embedding.BytesToVec(binVec, index.byteOrder)
		if err != nil {
			panic(err)
		}
		id := toColumnID(tableID, columnIndex)
		index.lsh.Insert(vec, id)
		count++
		fmt.Printf("\rLoaded %d embeddings into index", count)
	}
	fmt.Println()
}

func (index *SearchIndex) GetLSHIndexEntries() chan *LSHEntry {
	out := make(chan *LSHEntry)
	go func() {
		defer close(out)
		rows, err := index.db.Query(fmt.Sprintf(`
		SELECT table_id, column_index, vec FROM %s;
		`, index.tablename))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var tableID string
			var columnIndex int
			var binVec []byte
			if err := rows.Scan(&tableID, &columnIndex, &binVec); err != nil {
				panic(err)
			}
			vec, err := embedding.BytesToVec(binVec, index.byteOrder)
			if err != nil {
				panic(err)
			}
			hashKeys := index.lsh.toBasicHashTableKeys(index.lsh.hash(vec))
			for bandIndex, hashKey := range hashKeys {
				out <- &LSHEntry{
					TableID:     tableID,
					ColumnIndex: columnIndex,
					BandIndex:   bandIndex,
					HashKey:     hashKey,
				}
			}
		}
	}()
	return out
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
	var binVec []byte
	err := index.db.QueryRow(fmt.Sprintf(`
	SELECT vec FROM %s
	WHERE table_id=? AND column_index=?;
	`, index.tablename), tableID, columnIndex).Scan(&binVec)
	if err == sql.ErrNoRows {
		return nil, errors.New("The column does not exist in the index")
	}
	if err != nil {
		return nil, err
	}
	vec, err := embedding.BytesToVec(binVec, index.byteOrder)
	if err != nil {
		panic(err)
	}
	return &EmbEntry{
		TableID:     tableID,
		ColumnIndex: columnIndex,
		Vec:         vec,
	}, nil
}

func (index *SearchIndex) TopK(query []float64, k int) []*EmbEntry {
	lshResults := index.lsh.Query(query)
	log.Printf("LSH Returns %d candidates", len(lshResults))
	queue := NewTopKQueue(k)
	for _, id := range lshResults {
		tableID, columnIndex := fromColumnID(id)
		entry, err := index.Get(tableID, columnIndex)
		if err != nil {
			panic(err)
		}
		queue.Push(entry, dotProduct(query, entry.Vec))
	}
	result := make([]*EmbEntry, queue.Size())
	for i := len(result) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result[i] = v.(*EmbEntry)
	}
	return result
}

func (index *SearchIndex) checkSqlitTableExist() bool {
	var name string
	err := index.db.QueryRow(`
	SELECT name FROM sqlite_master WHERE type='table' AND name=?;
	`, index.tablename).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}
