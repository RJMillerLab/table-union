package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/embserver"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var searchIndexSqliteDB string
	var lshIndexSqliteDB string
	var l, m int
	var pcsNum int
	flag.StringVar(&searchIndexSqliteDB, "searchindex-db", "/home/fnargesian/WIKI_TABLE/search-index.db",
		"Sqlite database file for search index vecs")
	flag.StringVar(&lshIndexSqliteDB, "lshindex-db", "",
		"Output Sqlite database for LSH Index")
	flag.IntVar(&l, "l", 5, "LSH Parameter: number of bands or hash tables")
	flag.IntVar(&m, "m", 20, "LSH Parameter: size of each band or hash key")
	flag.IntVar(&pcsNum, "pcs", 3, "Number of principal components for representing a domain")
	flag.Parse()

	si := embserver.NewSearchIndex(nil, searchIndexSqliteDB,
		embserver.NewCosineLsh(fasttext.Dim, l, m), pcsNum)
	if si.IsNotBuilt() {
		panic("Search index is not built")
	}
	si.SaveLSHIndex(si.GetLSHIndexEntries(), lshIndexSqliteDB)
}
