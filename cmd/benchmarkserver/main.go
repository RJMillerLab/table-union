package main

import (
	"flag"
	"log"
	"os"

	"github.com/RJMillerLab/table-union/embserver"
	"github.com/RJMillerLab/table-union/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	FastTextDim = 300
)

func main() {
	var rebuildWikiTableStore bool
	var fastTextFilename string
	var fastTextSqliteDB string
	var searchIndexSqliteDB string
	var benchmarkDir string
	var rebuildSearchIndex bool
	var port string
	var l, m int
	flag.BoolVar(&rebuildWikiTableStore, "rebuild-wikitable", false,
		"Set to true to rebuild wikitable store, existing CSV files will be skipped")
	flag.StringVar(&fastTextFilename, "fasttext-raw", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec",
		"Facebook fastText word vec file")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs, will be created if not exist")
	flag.StringVar(&searchIndexSqliteDB, "searchindex-db", "/home/fnargesian/go/src/github.com/RJMillerLab/table-union/benchmark/search-index.db",
		"Sqlite database file for search index vecs, will be created if not exist")
	flag.StringVar(&benchmarkDir, "benchmark-dir", "/home/fnargesian/go/src/github.com/RJMillerLab/table-union/benchmark/testdata", "Directory for storing wikitable CSV files, will be created if not exist")
	flag.BoolVar(&rebuildSearchIndex, "rebuild-searchindex", false,
		"Set to true to rebuild search index from scratch, the existing search index sqlite database will be removed")
	flag.StringVar(&port, "port", "4004", "Server port")
	flag.IntVar(&l, "l", 5, "LSH Parameter: number of bands or hash tables")
	flag.IntVar(&m, "m", 20, "LSH Parameter: size of each band or hash key")
	flag.Parse()

	// Create Sqlite DB for fastText if not exists
	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		log.Print("Building FastText Sqlite database from scratch")
		f, err := os.Open(fastTextFilename)
		if err != nil {
			panic(err)
		}
		ft := fasttext.NewFastText(fastTextSqliteDB)
		if err := ft.BuildDB(f); err != nil {
			panic(err)
		}
		f.Close()
		ft.Close()
		log.Print("Finished building FastText Sqlite database")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)

	// Create wikitable store, build if not exists
	ts := wikitable.NewWikiTableStore(benchmarkDir)
	// No need to build CSV files
	if ts.IsNotBuilt() || rebuildWikiTableStore {
		if err := os.Remove(searchIndexSqliteDB); err != nil {
			panic(err)
		}
	}

	si := embserver.NewSearchIndex(ft, searchIndexSqliteDB, embserver.NewCosineLsh(FastTextDim, l, m))
	// Build search index if it is not built
	if si.IsNotBuilt() {
		log.Print("Building search index from scratch")
		if err := si.Build(ts); err != nil {
			panic(err)
		}
		log.Print("Finish building search index")
	}

	// Start server
	s := embserver.NewServer(ft, ts, si)
	defer s.Close()
	s.Run(port)
}
