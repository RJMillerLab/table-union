package table

import (
	"os"
	"testing"
)

func TestOntBuild(t *testing.T) {
	ts := NewTableStore("testdata/ontwikitables")
	f, err := os.Open("testdata/tables.json")
	if err != nil {
		t.Fail()
	}
	if err := ts.OntBuild(f); err != nil {
		t.Fail()
	}
	f.Close()
}
