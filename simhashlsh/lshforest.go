package simhashlsh

import (
	"log"
	"math"
	"sort"
	"sync"
)

const (
	integrationPrecision = 0.01
)

// Key is a way to index into a table.
type hashTableKey []uint8

// Value is an index into the input dataset.
type hashTableBucket []string

//type hashTable map[uint64]hashTableBucket

type keys []string

// For initial bootstrapping
type initHashTable map[string]keys

type bucket struct {
	hashKey string
	keys    keys
}

type hashTable []bucket

func (h hashTable) Len() int           { return len(h) }
func (h hashTable) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h hashTable) Less(i, j int) bool { return h[i].hashKey < h[j].hashKey }

type cosineLSHParam struct {
	// Dimensionality of the input data.
	dim int
	// Number of hash tables.
	l int
	// Number of hash functions for each table.
	k int
	// Hyperplanes
	hyperplanes [][]float64
	// Number of hash functions
	numHash int
}

// NewLshParams initializes the LSH settings.
func newCosineLSHParam(dim, l, k, numHash int, hyperplanes [][]float64) *cosineLSHParam {
	return &cosineLSHParam{
		dim:         dim,
		l:           l,
		k:           k,
		hyperplanes: hyperplanes,
		numHash:     numHash,
	}
}

type CosineLSH struct {
	// Param type
	*cosineLSHParam
	// Tables
	tables     []hashTable
	initTables []initHashTable
}

// Compute the integral of function f, lower limit a, upper limit l, and
// precision defined as the quantize step
func integral(f func(float64) float64, a, b, precision float64) float64 {
	var area float64
	for x := a; x < b; x += precision {
		area += f(x+0.5*precision) * precision
	}
	return area
}

// Probability density function for false positive
func falsePositive(l, k int) func(float64) float64 {
	return func(j float64) float64 {
		return 1.0 - math.Pow(1.0-math.Pow(j, float64(k)), float64(l))
	}
}

// Probability density function for false negative
func falseNegative(l, k int) func(float64) float64 {
	return func(j float64) float64 {
		return 1.0 - (1.0 - math.Pow(1.0-math.Pow(j, float64(k)), float64(l)))
	}
}

// Compute the cummulative probability of false negative given threshold t
func probFalseNegative(l, k int, t, precision float64) float64 {
	return integral(falseNegative(l, k), t, 1.0, precision)
}

// Compute the cummulative probability of false positive given threshold t
func probFalsePositive(l, k int, t, precision float64) float64 {
	return integral(falsePositive(l, k), 0, t, precision)
}

// optimalKL returns the optimal K and L for Jaccard similarity search,
// and the false positive and negative probabilities.
// t is the Jaccard similarity threshold.
func optimalKL(numHash int, t float64) (optK, optL int, fp, fn float64) {
	minError := math.MaxFloat64
	for l := 1; l <= numHash; l++ {
		for k := 1; k <= numHash; k++ {
			if l*k > numHash {
				continue
			}
			currFp := probFalsePositive(l, k, t, integrationPrecision)
			currFn := probFalseNegative(l, k, t, integrationPrecision)
			currErr := currFn + currFp
			if minError > currErr {
				minError = currErr
				optK = k
				optL = l
				fp = currFp
				fn = currFn
			}
		}
	}
	log.Printf("optK: %d, optL: %d", optK, optL)
	return
}

// NewCosineLSH created an instance of Cosine LSH.
// dim is the number of dimensions of the input points (also the number of dimensions of each hyperplane)
// l is the number of hash tables, m is the number of hash values in each hash table.
func NewCosineLSH(dim, numHash int, threshold float64) *CosineLSH {
	hyperplanes := NewHyperplanes(numHash, dim)
	k, l, _, _ := optimalKL(numHash, threshold)

	tables := make([]hashTable, l)
	for i := range tables {
		tables[i] = make(hashTable, 0)
	}
	initTables := make([]initHashTable, l)
	for i := range initTables {
		initTables[i] = make(initHashTable)
	}
	return &CosineLSH{
		cosineLSHParam: newCosineLSHParam(dim, l, k, numHash, hyperplanes),
		tables:         tables,
		initTables:     initTables,
	}
}

// Params returns the LSH parameters k and l
func (f *CosineLSH) Params() (k, l, dim int) {
	return f.cosineLSHParam.k, f.cosineLSHParam.l, f.cosineLSHParam.dim
}

// Add a key with SimHash signature into the index.
func (index *CosineLSH) Add(point []float64, key string) {
	// Apply hash functions
	Hs := index.toBasicHashTableKeys(index.hash(point))
	// Insert keys into the bootstrapping tables
	var wg sync.WaitGroup
	wg.Add(len(index.initTables))
	for i := range index.initTables {
		go func(ht initHashTable, hk, key string) {
			if _, exist := ht[hk]; exist {
				ht[hk] = append(ht[hk], key)
			} else {
				ht[hk] = make(keys, 1)
				ht[hk][0] = key
			}
			wg.Done()
		}(index.initTables[i], Hs[i], key)
	}
	wg.Wait()
}

// Makes all the keys added searchable.
func (index *CosineLSH) Index() {
	log.Printf("Indexing the columns")
	var wg sync.WaitGroup
	wg.Add(len(index.tables))
	for i := range index.tables {
		go func(htPtr *hashTable, initHtPtr *initHashTable) {
			// Build sorted hash table using buckets from init hash tables
			initHt := *initHtPtr
			ht := *htPtr
			for hashKey := range initHt {
				ks, _ := initHt[hashKey]
				ht = append(ht, bucket{
					hashKey: hashKey,
					keys:    ks,
				})
			}
			sort.Sort(ht)
			*htPtr = ht
			// Reset the init hash tables
			*initHtPtr = make(initHashTable)
			wg.Done()
		}(&(index.tables[i]), &(index.initTables[i]))
	}
	wg.Wait()
	log.Printf("Done indexing")
}

// Query finds the ids of approximate nearest neighbour candidates,
// in un-sorted order, given the query point.
// Return candidate keys given the query signature.
func (index *CosineLSH) Query(point []float64) []string {
	result := make([]string, 0)
	done := make(chan struct{})
	for key := range index.query(point, index.cosineLSHParam.k, done) {
		result = append(result, key)
	}
	return result
}

func (index *CosineLSH) query(point []float64, minK int, done <-chan struct{}) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		// Generate hash keys
		Hs := index.toBasicHashTableKeys(index.hash(point))
		seens := make(map[string]bool)
		for K := index.cosineLSHParam.k; K >= minK; K-- {
			prefixSize := K
			// Query hash tables in parallel
			keyChan := make(chan string)
			var wg sync.WaitGroup
			wg.Add(index.cosineLSHParam.l)
			for i := 0; i < index.cosineLSHParam.l; i++ {
				go func(ht hashTable, hk string) {
					defer wg.Done()
					k := sort.Search(len(ht), func(x int) bool {
						return ht[x].hashKey[:prefixSize] >= hk
					})
					if k < len(ht) && ht[k].hashKey[:prefixSize] == hk {
						for j := k; j < len(ht) && ht[j].hashKey[:prefixSize] == hk; j++ {
							for _, key := range ht[j].keys {
								select {
								case keyChan <- key:
								case <-done:
									return
								}
							}
						}
					}
				}(index.tables[i], Hs[i])
			}
			go func() {
				wg.Wait()
				close(keyChan)
			}()
			for key := range keyChan {
				if _, seen := seens[key]; seen {
					continue
				}
				out <- key
				seens[key] = true
			}
		}
	}()
	return out
}

// Hash returns all combined hash values for all hash tables.
func (clsh *cosineLSHParam) hash(point []float64) []hashTableKey {
	sig := newSimhash(clsh.hyperplanes, point).sig
	hvs := make([]hashTableKey, clsh.l)
	for i := range hvs {
		s := make(hashTableKey, clsh.k)
		for j := 0; j < clsh.k; j++ {
			s[j] = uint8(sig[i*clsh.k+j])
		}
		hvs[i] = s
	}
	return hvs
}

func (index *CosineLSH) toBasicHashTableKeys(keys []hashTableKey) []string { // []uint64 {
	basicKeys := make([]string, index.cosineLSHParam.l)
	for i, key := range keys {
		s := ""
		for _, hashVal := range key {
			switch hashVal {
			case uint8(0):
				s += "0"
			case uint8(1):
				s += "1"
			default:
				panic("Hash value is not 0 or 1")
			}
		}
		basicKeys[i] = s
	}
	return basicKeys
}
