package embserver

import (
	"log"
	"testing"

	minhashlsh "github.com/ekzhu/minhash-lsh"
)

func Test_Jaccard(t *testing.T) {
	d1 := []string{"a", "a", "a", "b"}
	d2 := []string{"B", "c", "c", "d"}
	if jaccard(d1, d2) != 0.25 {
		t.Fail()
	}
}

func Test_Containment(t *testing.T) {
	d1 := []string{"a", "a", "a", "b"}
	d2 := []string{"B", "c", "c", "d"}
	if containment(d1, d2) != 0.5 {
		t.Fail()
	}
}

func Test_Lsh(t *testing.T) {
	minhashLsh := minhashlsh.NewMinhashLSH32(256, 0.6)
	sig, err := readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/6bfc5e0c-86e8-4451-a317-d95d110e1102", 9, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/6bfc5e0c-86e8-4451-a317-d95d110e1102", 9), sig)
	sig, err = readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/28e984db-3d5e-4fe8-9d7b-8146be7e5efc", 9, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/28e984db-3d5e-4fe8-9d7b-8146be7e5efc", 9), sig)
	sig, err = readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/252f152f-a97a-435d-a15d-14c70de139fc", 8, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/252f152f-a97a-435d-a15d-14c70de139fc", 8), sig)
	//query := []string{"Gouvernement provincial", "Entreprise", "Entreprise"}
	//qmh := getMinhash(query, 1, 256)
	results := minhashLsh.Query(sig)
	log.Printf("%v", results)
	if len(results) < 1 {
		t.Fail()
	}
}
