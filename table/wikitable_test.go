package table

import (
	"log"
	"os"
	"testing"
)

func TestReadWikiTable(t *testing.T) {
	wikiTableDir := "./testdata/wikitables"
	ts := NewTableStore(wikiTableDir)
	f, err := os.Open("./testdata/tables.json")
	if err != nil {
		t.Error(err)
	}
	if err := ts.BuildWT(f); err != nil {
		panic(err)
	}
	f.Close()
	log.Print("Finished building wikitable store")
}
