package wikitable

import (
	"os"
	"testing"
)

func TestReadWikiTable(t *testing.T) {
	f, err := os.Open("./testdata/tables.json")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()
	//for table := range ReadWikiTable(f) {
	//	t.Log(table.ID)
	//	t.Log(table.Headers)
	//	t.Log(table.Columns)
	//}
}
