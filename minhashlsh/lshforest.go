package minhashlsh

import (
	"encoding/binary"
	"math"
	"sort"
	"sync"
)

const (
	integrationPrecision = 0.01
)

type UnionPair struct {
	QueryIndex   int
	CandidateKey string
}

type hashKeyFunc func(Signature) string

func hashKeyFuncGen(hashValueSize int) hashKeyFunc {
	return func(sig Signature) string {
		s := make([]byte, hashValueSize*len(sig))
		buf := make([]byte, 8)
		for i, v := range sig {
			binary.LittleEndian.PutUint64(buf, v)
			copy(s[i*hashValueSize:(i+1)*hashValueSize], buf[:hashValueSize])
		}
		return string(s)
	}
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
	return
}

// Default constructor uses 32 bit hash value
var NewMinhashLSH = NewMinhashLSH32

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

// MinhashLSH represents a MinHash LSH implemented using LSH Forest
// (http://ilpubs.stanford.edu:8090/678/1/2005-14.pdf).
// It supports query-time setting of the MinHash LSH parameters
// L (number of bands) and
// K (number of hash functions per band).
type MinhashLSH struct {
	k              int
	l              int
	initHashTables []initHashTable
	hashTables     []hashTable
	hashKeyFunc    hashKeyFunc
	hashValueSize  int
}

func newMinhashLSH(threshold float64, numHash, hashValueSize int) *MinhashLSH {
	k, l, _, _ := optimalKL(numHash, threshold)
	hashTables := make([]hashTable, l)
	for i := range hashTables {
		hashTables[i] = make(hashTable, 0)
	}
	initHashTables := make([]initHashTable, l)
	for i := range initHashTables {
		initHashTables[i] = make(initHashTable)
	}
	return &MinhashLSH{
		k:              k,
		l:              l,
		hashValueSize:  hashValueSize,
		initHashTables: initHashTables,
		hashTables:     hashTables,
		hashKeyFunc:    hashKeyFuncGen(hashValueSize),
	}
}

// NewMinhashLSH64 uses 64-bit hash values.
func NewMinhashLSH64(numHash int, threshold float64) *MinhashLSH {
	return newMinhashLSH(threshold, numHash, 8)
}

// NewMinhashLSH32 uses 32-bit hash values.
// MinHash signatures with 64 bit hash values will have
// their hash values trimed.
func NewMinhashLSH32(numHash int, threshold float64) *MinhashLSH {
	return newMinhashLSH(threshold, numHash, 4)
}

// NewMinhashLSH16 uses 16-bit hash values.
// MinHash signatures with 64 or 32 bit hash values will have
// their hash values trimed.
func NewMinhashLSH16(numHash int, threshold float64) *MinhashLSH {
	return newMinhashLSH(threshold, numHash, 2)
}

// Add a key with MinHash signature into the index.
// The key won't be searchable until Index() is called.
func (f *MinhashLSH) Add(key string, sig Signature) {
	// Generate hash keys
	Hs := make([]string, f.l)
	for i := 0; i < f.l; i++ {
		Hs[i] = f.hashKeyFunc(sig[i*f.k : (i+1)*f.k])
	}
	// Insert keys into the bootstrapping tables
	var wg sync.WaitGroup
	wg.Add(len(f.initHashTables))
	for i := range f.initHashTables {
		go func(ht initHashTable, hk, key string) {
			if _, exist := ht[hk]; exist {
				ht[hk] = append(ht[hk], key)
			} else {
				ht[hk] = make(keys, 1)
				ht[hk][0] = key
			}
			wg.Done()
		}(f.initHashTables[i], Hs[i], key)
	}
	wg.Wait()
}

// Makes all the keys added searchable.
func (f *MinhashLSH) Index() {
	var wg sync.WaitGroup
	wg.Add(len(f.hashTables))
	for i := range f.hashTables {
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
		}(&(f.hashTables[i]), &(f.initHashTables[i]))
	}
	wg.Wait()
}

func (f *MinhashLSH) QueryPlus(sigs []Signature, done <-chan struct{}) <-chan UnionPair {
	return f.query(sigs, done)
}

func (f *MinhashLSH) query(sigs []Signature, done <-chan struct{}) <-chan UnionPair {
	out := make(chan UnionPair)
	seens := make(map[UnionPair]bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for K := f.k; K > 0; K-- {
			prefixSize := f.hashValueSize * K
			// Generate hash keys
			Hs := make([][]string, len(sigs))
			for j := 0; j < len(sigs); j++ {
				SHs := make([]string, f.l)
				for i := 0; i < f.l; i++ {
					SHs[i] = f.hashKeyFunc(sigs[j][i*f.k : i*f.k+K])
				}
				Hs[j] = SHs
			}
			for rp := range f.probe(Hs, prefixSize, done) {
				if _, seen := seens[rp]; seen {
					continue
				}
				seens[rp] = true
				out <- rp
			}
		}
	}()
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

//func (f *MinhashLSH) query(sig Signature, minK int, done <-chan struct{}) <-chan string {
func (f *MinhashLSH) probe(Hs [][]string, prefixSize int, done <-chan struct{}) <-chan UnionPair {
	out := make(chan UnionPair)
	var wg sync.WaitGroup
	wg.Add(f.l * len(Hs))
	go func() {
		for i := 0; i < f.l; i++ {
			for p := 0; p < len(Hs); p++ {
				go func(i, p int) {
					hk := Hs[p][i]
					ht := f.hashTables[i]
					defer wg.Done()
					k := sort.Search(len(ht), func(x int) bool {
						return ht[x].hashKey[:prefixSize] >= hk
					})
					if k < len(ht) && ht[k].hashKey[:prefixSize] == hk[:prefixSize] {
						for j := k; j < len(ht) && ht[j].hashKey[:prefixSize] == hk; j++ {
							for _, key := range ht[j].keys {
								rp := UnionPair{
									QueryIndex:   p,
									CandidateKey: key,
								}
								select {
								case out <- rp:
								case <-done:
									return
								}
							}
						}
					}
				}(i, p)
			}
		}
	}()
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
