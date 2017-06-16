package unionserver

import (
	"log"
	"os"
	"testing"

	"github.com/RJMillerLab/table-union/embedding"
)

func Test_Alignment(t *testing.T) {
	qtable := "open.canada.ca_data_en.jsonl/51e2385e-1ec8-468b-8dde-f736e76ac7c3/38da135d-3d2f-4d01-b8c4-ffb0097fe744____0/05770001-eng.csv"
	domainDir := "/home/fnargesian/TABLE_UNION_OUTPUT"
	query := make([][]float64, 0)
	tds := getTextDomains(qtable, domainDir)
	for _, index := range tds {
		embFilename := getEmbFilename(qtable, domainDir, index)
		if _, err := os.Stat(embFilename); os.IsNotExist(err) {
			log.Printf("Embedding file %s does not exist.", embFilename)
			continue
		}
		vec, err := embedding.ReadVecFromDisk(embFilename, ByteOrder)
		if err != nil {
			log.Printf("Error in reading %s from disk.", embFilename)
			continue
		}
		query = append(query, vec)
	}
	cand := "open.canada.ca_data_en.jsonl/51e2385e-1ec8-468b-8dde-f736e76ac7c3/38da135d-3d2f-4d01-b8c4-ffb0097fe744____0/05770001-eng.csv"
	union := Align(cand, domainDir, query, len(tds))
	log.Printf("k-unionability score: %f", union.Kunioability)
	log.Printf("Number of aligned columns: %d", len(union.Alignment))
	log.Printf("Number of text columns in the query: %d", len(tds))
	log.Printf("Alignment: %v", union.Alignment)
	if len(union.Alignment) != len(tds) {
		log.Printf("%d vs %d", len(union.Alignment), len(tds))
		t.Fail()
	}
}
