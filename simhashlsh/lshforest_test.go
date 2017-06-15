package simhashlsh

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
)

func Test_LSHQuery(t *testing.T) {
	ts := []float64{0.3, 0.5, 0.7, 0.9}
	vecs := randomVectors(100, 300, 1.0)
	for _, t := range ts {
		log.Printf("Querying with threhsold %f.", t)
		var avg float64
		clsh := NewCosineLSH(300, 100, t)
		insertedVectors := make([]string, 100)
		for i, e := range vecs {
			clsh.Add(e, strconv.Itoa(i))
			insertedVectors[i] = strconv.Itoa(i)
		}
		clsh.Index()
		// Use the inserted vectors as queries, and
		// verify that we can get back each query itself
		for i, key := range insertedVectors {
			results := clsh.Query(vecs[i])
			avg += float64(len(results))
			found := false
			for _, foundKey := range results {
				if foundKey == key {
					found = true
				}
			}
			//			log.Printf("number of results: %d", len(results))
			if !found {
				log.Println("No results.")
			}
		}
		log.Printf("avg number of returned results for each query %f", avg/float64(len(vecs)))
	}
}

func Test_LSHQueryPlus(t *testing.T) {
	ts := []float64{0.3, 0.5, 0.7, 0.9}
	vecs := randomVectors(100, 300, 1.0)
	for _, t := range ts {
		log.Printf("Querying with threhsold %f.", t)
		var avg float64
		clsh := NewCosineLSH(300, 100, t)
		insertedVectors := make([]string, 100)
		for i, e := range vecs {
			clsh.Add(e, strconv.Itoa(i))
			insertedVectors[i] = strconv.Itoa(i)
		}
		clsh.Index()
		// Use the inserted vectors as queries, and
		// verify that we can get back each query itself
		for i := 0; i < len(insertedVectors); i += 2 {
			key1 := insertedVectors[i]
			key2 := insertedVectors[i+1]
			results := clsh.QueryPlus(vecs[i : i+2])
			avg += float64(len(results))
			found := 0
			for _, foundPair := range results {
				if (foundPair.queryIndex == 0 && foundPair.candidateKey == key1) || (foundPair.queryIndex == 1 && foundPair.candidateKey == key2) {
					found += 1
				}
			}
			log.Printf("number of results: %d", len(results))
			if found < 2 {
				log.Printf("found only %d results.", found)
			}
		}
		log.Printf("avg number of returned results for each query %f", avg/float64(len(vecs)))
	}
}

func randomVectors(n, dim int, max float64) [][]float64 {
	random := rand.New(rand.NewSource(1))
	vecs := make([][]float64, n)
	for i := 0; i < n; i++ {
		vecs[i] = make([]float64, dim)
		for d := 0; d < dim; d++ {
			vecs[i][d] = random.Float64() * max
		}
	}
	return vecs
}
