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

func Test_Lsh1(t *testing.T) {
	minhashLsh := minhashlsh.NewMinhashLSH64(256, 0.6)
	sig1, err := readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 1, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 1), sig1)
	sig2, err := readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 2, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 2), sig2)
	sig3, err := readMinhashSignature("/home/fnargesian/TABLE_UNION_OUTPUT/domains", "open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 3, 256)
	if err != nil {
		log.Printf("Could not find minhash")
	}
	minhashLsh.Add(getDomainName("open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe", 3), sig3)
	minhashLsh.Index()
	results := minhashLsh.Query(sig3)
	log.Printf("%v", results)
	if len(results) < 1 {
		t.Fail()
	}
}

func Test_Lsh2(t *testing.T) {
	minhashLsh := minhashlsh.NewMinhashLSH16(256, 0.1)
	seed = 1
	numHash = 256
	mh := minhashlsh.NewMinhash(seed, numHash)
	words := []string{"hello", "world", "minhash"}
	for _, word := range words {
		mh.Push([]byte(word))
	}
	sig1 := mh.Signature()
	minhashLsh.Add("s1", sig1)

	mh = minhashlsh.NewMinhash(seed, numHash)
	words = []string{"hello", "minhash"}
	for _, word := range words {
		mh.Push([]byte(word))
	}
	sig2 := mh.Signature()
	minhashLsh.Add("s2", sig2)

	mh = minhashlsh.NewMinhash(seed, numHash)
	words = []string{"world", "minhash"}
	for _, word := range words {
		mh.Push([]byte(word))
	}
	sig3 := mh.Signature()
	minhashLsh.Add("s3", sig3)
	minhashLsh.Index()
	results := minhashLsh.Query(sig3)
	log.Printf("%v", results)
	if len(results) < 1 {
		t.Fail()
	}
}
