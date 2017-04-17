package main

import (
	"flag"
	"log"
	"os"

	"github.com/RJMillerLab/table-union/embserver"
	"github.com/RJMillerLab/table-union/table"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	FastTextDim = 300
)

func main() {
	var fastTextFilename string
	var fastTextSqliteDB string
	var openDataFilename string
	var searchIndexSqliteDB string
	var openDataDir string
	var rebuildSearchIndex bool
	var rebuildOpenDataStore bool
	var port string
	var l, m int
	var pcsNum int
	flag.StringVar(&fastTextFilename, "fasttext-raw", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec",
		"Facebook fastText word vec file")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs, will be created if not exist")
	flag.StringVar(&openDataFilename, "opendata-raw", "/home/fnargesian/OPENDATA/cod.csv",
		"open dataset file")
	// "/home/fnargesian/OPENDATA/csv_files_2016-12-15-canada.csv"
	flag.StringVar(&searchIndexSqliteDB, "searchindex-db", "/home/fnargesian/OPENDATA/search-index.db",
		"sqlite database file for search index vecs, will be created if not exist")
	flag.StringVar(&openDataDir, "opendata-dir", "/home/fnargesian/OPENDATA/datasets",
		"Directory for storing opendata CSV files, will be created if not exist")
	flag.BoolVar(&rebuildOpenDataStore, "rebuild-opendata", false,
		"Set to true to rebuild opendata store, existing CSV files will be skipped")
	flag.BoolVar(&rebuildSearchIndex, "rebuild-searchindex", false,
		"Set to true to rebuild search index from scratch, the existing search index sqlite database will be removed")
	flag.StringVar(&port, "port", "4003", "Server port")
	flag.IntVar(&l, "l", 5, "LSH Parameter: number of bands or hash tables")
	flag.IntVar(&m, "m", 20, "LSH Parameter: size of each band or hash key")
	flag.IntVar(&pcsNum, "pcs", 3, "Number of principal components for representing a domain")
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
	defer ft.Close()

	// Create opendata store, build if not exists
	ts := table.NewTableStore(openDataDir)
	if ts.IsNotBuilt() || rebuildOpenDataStore {
		log.Print("Building opendata store")
		f, err := os.Open(openDataFilename)
		if err != nil {
			panic(err)
		}
		if err := ts.BuildOD(f); err != nil {
			panic(err)
		}
		f.Close()
		log.Print("Finish building open data store")
	}

	if rebuildSearchIndex {
		if err := os.Remove(searchIndexSqliteDB); err != nil {
			panic(err)
		}
	}

	si := embserver.NewSearchIndex(ft, searchIndexSqliteDB, embserver.NewCosineLsh(FastTextDim, l, m), pcsNum)
	// Build search index if it is not built
	if si.IsNotBuilt() {
		log.Print("Building search index from scratch")
		if err := si.Build(ts); err != nil {
			log.Print(err)
			panic(err)
		}
		log.Print("Finish building search index")
	} else {
		si.Load()
	}

	// Start server
	s := embserver.NewServer(ft, ts, si)
	defer s.Close()
	s.Run(port)
}
