package wikitable

import (
	"os"
	"testing"
)

func TestOntBuild(t *testing.T) {
	ts := NewWikiTableStore("testdata/wikitables")
	f, err := os.Open("testdata/tables.json")
	if err != nil {
		t.Fail()
	}
	if err := ts.OntBuild(f); err != nil {
		t.Fail()
	}
	f.Close()
}
