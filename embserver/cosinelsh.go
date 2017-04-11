package embserver

import (
	"math/rand"
	"strconv"
	"sync"
)

type signature []uint8

// Key is a way to index into a table.
type hashTableKey []uint8

// Value is an index into the input dataset.
type hashTableBucket []string

type hashTable map[uint64]hashTableBucket

// Represents a SinHash signature - an array of hash values
type simhash struct {
	sig signature
}

//signature generates the simhash of an attribute (a bag of values) using the hyperplanes
// Initialize a MinHash object with a seed and the number of
// hash functions.
func newSimhash(hs hyperplanes, e []float64) *simhash {
	sig := newSignature(hs, e)
	return &simhash{
		sig: sig,
	}
}

func newSignature(hyperplanes hyperplanes, e []float64) signature {
	sigarr := make([]uint8, len(hyperplanes))
	for hix, h := range hyperplanes {
		var dp float64
		for k, v := range e {
			dp += h[k] * float64(v)
		}
		if dp >= 0 {
			sigarr[hix] = uint8(1)
		} else {
			sigarr[hix] = uint8(0)
		}
	}
	return sigarr
}

// the hyperplanes
type hyperplanes [][]float64

//NewHyperplanes generates and initializes a set of d hyperplanes with s dimensions.
func newHyperplanes(d, s int) hyperplanes {
	hs := make([][]float64, d)
	for i := 0; i < d; i++ {
		v := make([]float64, s)
		for i := 0; i < s; i++ {
			n := rand.NormFloat64()
			v[i] = n
		}
		hs[i] = v
	}
	return hs
	//return generateHPs(d, s)
}

type cosineLshParam struct {
	// Dimensionality of the input data.
	dim int
	// Number of hash tables.
	l int
	// Number of hash functions for each table.
	m int
	// Hyperplanes
	hyperplanes [][]float64
	// Number of hash functions
	h int
}

// NewLshParams initializes the LSH settings.
func newCosineLshParam(dim, l, m, h int, hyperplanes [][]float64) *cosineLshParam {
	return &cosineLshParam{
		dim:         dim,
		l:           l,
		m:           m,
		hyperplanes: hyperplanes,
		h:           h,
	}
}

// Hash returns all combined hash values for all hash tables.
func (clsh *cosineLshParam) hash(point []float64) []hashTableKey {
	simhash := newSimhash(clsh.hyperplanes, point)
	hvs := make([]hashTableKey, clsh.l)
	for i := range hvs {
		s := make(hashTableKey, clsh.m)
		for j := 0; j < clsh.m; j++ {
			s[j] = uint8(simhash.sig[i*clsh.m+j])
		}
		hvs[i] = s
	}
	return hvs
}

type CosineLsh struct {
	// Param type
	*cosineLshParam
	// Tables
	tables []hashTable
}

// NewCosineLsh created an instance of Cosine LSH.
// dim is the number of dimensions of the input points (also the number of dimensions of each hyperplane)
// l is the number of hash tables, m is the number of hash values in each hash table.
func NewCosineLsh(dim, l, m int) *CosineLsh {
	h := m * l
	hyperplanes := newHyperplanes(h, dim)
	tables := make([]hashTable, l)
	for i := range tables {
		tables[i] = make(hashTable)
	}
	return &CosineLsh{
		cosineLshParam: newCosineLshParam(dim, l, m, h, hyperplanes),
		tables:         tables,
	}
}

// Insert adds a new data point to the Cosine LSH.
// point is a data point being inserted into the index and
// id is the unique identifier for the data point.
func (index *CosineLsh) Insert(point []float64, id string) {
	// Apply hash functions
	hvs := index.toBasicHashTableKeys(index.hash(point))
	// Insert key into all hash tables
	var wg sync.WaitGroup
	wg.Add(len(index.tables))
	for i := range index.tables {
		hv := hvs[i]
		table := index.tables[i]
		go func(table hashTable, hv uint64) {
			if _, exist := table[hv]; !exist {
				table[hv] = make(hashTableBucket, 0)
			}
			table[hv] = append(table[hv], id)
			wg.Done()
		}(table, hv)
	}
	wg.Wait()
}

// Query finds the ids of approximate nearest neighbour candidates,
// in un-sorted order, given the query point.
func (index *CosineLsh) Query(q []float64) []string {
	// Apply hash functions
	hvs := index.toBasicHashTableKeys(index.hash(q))
	// Keep track of keys seen
	seen := make(map[string]bool)
	for i, table := range index.tables {
		if candidates, exist := table[hvs[i]]; exist {
			for _, id := range candidates {
				if _, exist := seen[id]; exist {
					continue
				}
				seen[id] = true
			}
		}
	}
	// Collect results
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

func (index *CosineLsh) toBasicHashTableKeys(keys []hashTableKey) []uint64 {
	basicKeys := make([]uint64, index.cosineLshParam.l)
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
		v, err := strconv.ParseUint(s, 2, 64)
		if err != nil {
			panic(err)
		}
		basicKeys[i] = v
	}
	return basicKeys
}
