package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/RJMillerLab/fastTextHomeWork/search"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var fastTextFilename string
	var sqliteDbFilename string
	var wikiTableFilename string
	flag.StringVar(&fastTextFilename, "fasttext-raw", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec",
		"Facebook fastText word vec file")
	flag.StringVar(&sqliteDbFilename, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs, will be created if not exist")
	flag.StringVar(&wikiTableFilename, "wikitable-raw", "/home/ekzhu/WIKI_TABLE/tables.json",
		"WikiTable dataset file")
	flag.Parse()

	// Create Sqlite DB for fastText if not exists
	if _, err := os.Stat(sqliteDbFilename); os.IsNotExist(err) {
		f, err := os.Open(fastTextFilename)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := fasttext.NewFastText(sqliteDbFilename).BuildDB(f); err != nil {
			panic(err)
		}
	}

	ft := fasttext.NewFastText(sqliteDbFilename)
	index := search.NewSearchIndex(ft)

	// Build search index
	// TODO: make this step faster by initializing from a sqlite db
	// similar to fastText
	// See TODOs in search.go
	f, err := os.Open(wikiTableFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := index.Build(f); err != nil {
		panic(err)
	}

	// Test
	// TODO: use a file (e.g., CSV) as input instead
	qstr := []string{"ottawa", "toronto", "montreal"}
	// Build the embedding vector of the test words
	vec, err := index.GetEmb(qstr)
	if err != nil {
		panic(err)
	}
	// query
	result := index.TopK(vec, 5)
	for _, v := range result {
		fmt.Printf("Table %d, Column %d\n", v.TableID, v.ColumnIndex)
		index.GetTable(v.TableID).ToCsv(os.Stdout)
	}
}
