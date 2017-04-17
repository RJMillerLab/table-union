package table

import (
	"log"
	"os"
	"testing"
)

func TestReadOpenData(t *testing.T) {
	openDataDir := "./testdata/opendatasets"
	ts := NewTableStore(openDataDir)
	f, err := os.Open("./testdata/cod.csv")
	if err != nil {
		t.Error(err)
	}
	if err := ts.BuildOD(f); err != nil {
		panic(err)
	}
	f.Close()
	log.Print("Finish building open store")
}
