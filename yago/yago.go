package yago

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

var notAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

// Yago provides a way to annotate data value
// with entity from YAGO ontology.
type Yago struct {
	db *sql.DB
}

func InitYago(filename string) *Yago {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`
		ATTACH DATABASE '%s' AS disk;`, filename))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE VIRTUAL TABLE entities USING fts5(entity);")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`
		INSERT INTO entities(entity)
		SELECT distinct(entity) FROM disk.types;`)
	if err != nil {
		panic(err)
	}
	return &Yago{db}
}

// Create a copy of Yago.
// This is important when use Yago in multi-threading
// settings. Each thread must use its own copy of Yago.
func (y *Yago) Copy() *Yago {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	return &Yago{db}
}

func (y *Yago) Close() error {
	return y.db.Close()
}

// MatchEntity attempts to find entities whose
// names match the given data value.
func (y *Yago) MatchEntity(data string, limit int) (results []string) {
	results = make([]string, 0)
	data = strings.TrimSpace(notAlphaNumeric.ReplaceAllString(data, " "))
	if data == "" {
		return
	}
	rows, err := y.db.Query(`
		SELECT entity FROM entities WHERE entities MATCH ?
		ORDER BY rank LIMIT ?;`, data, limit)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var entity string
		if err := rows.Scan(&entity); err != nil {
			panic(err)
		}
		results = append(results, entity)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return
}
