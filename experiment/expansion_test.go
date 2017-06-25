package experiment

import (
	"testing"
)

func Test_Expansion(t *testing.T) {
	t1Tablename := "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/000fe5aa-1d77-42d1-bfe7-458c51dacfef/12c0b635-82de-42fa-8bbc-c5a5da82d4c5"
	exp1 := ComputeColumnExpansion(t1Tablename, t1Tablename, 1, 1)
	if exp1.NumUniqueValuesAdded != 0 {
		t.Fail()
	}
	exp2 := ComputeColumnExpansion(t1Tablename, t1Tablename, 1, 2)
	if exp2.NumUniqueValuesAdded == 0 {
		t.Fail()
	}
}
