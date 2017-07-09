package simhashlsh

import (
	"log"
	"sort"
	"sync"
)

type UnionPair struct {
	QueryIndex   int
	CandidateKey string
}

// Query finds the ids of approximate nearest neighbour candidates,
// in un-sorted order, given a list of points.
// Return candidate keys given the query signatures.
func (index *CosineLSH) QueryPlus(point [][]float64, done <-chan struct{}) <-chan UnionPair {
	return index.queryPlusPlus(point, done)
}

func (index *CosineLSH) queryPlus(points [][]float64, done <-chan struct{}) <-chan UnionPair {
	out := make(chan UnionPair)
	go func() {
		defer close(out)
		seens := make(map[UnionPair]bool)
		// Generate hash keys
		Hs := make([][]string, len(points))
		for i := 0; i < len(points); i++ {
			Hs[i] = index.toBasicHashTableKeys(index.hash(points[i]))
		}
		for K := index.cosineLSHParam.k; K > 0; K-- {
			prefixSize := K
			// Query hash tables in parallel
			keyChan := make(chan UnionPair)
			var wg sync.WaitGroup
			wg.Add(index.cosineLSHParam.l * len(points))
			for i := 0; i < index.cosineLSHParam.l; i++ {
				for p := 0; p < len(Hs); p++ {
					go func(ht hashTable, hk string, q int) {
						defer wg.Done()
						k := sort.Search(len(ht), func(x int) bool {
							return ht[x].hashKey[:prefixSize] >= hk[:prefixSize]
						})
						if k < len(ht) && ht[k].hashKey[:prefixSize] == hk[:prefixSize] {
							for j := k; j < len(ht) && ht[j].hashKey[:prefixSize] == hk[:prefixSize]; j++ {
								for _, key := range ht[j].keys {
									rp := UnionPair{
										QueryIndex:   q,
										CandidateKey: key,
									}
									select {
									case keyChan <- rp:
									case <-done:
										return
									}
								}
							}
						}
					}(index.tables[i], Hs[p][i], p)
				}
			}
			go func() {
				wg.Wait()
				close(keyChan)
			}()
			for qk := range keyChan {
				if _, seen := seens[qk]; seen {
					continue
				}
				out <- qk
				seens[qk] = true
			}
		}
	}()
	return out
}

func (index *CosineLSH) queryPlusPlus(points [][]float64, done <-chan struct{}) <-chan UnionPair {
	out := make(chan UnionPair)
	// Generate hash keys
	Hs := make([][]string, len(points))
	for i := 0; i < len(points); i++ {
		Hs[i] = index.toBasicHashTableKeys(index.hash(points[i]))
	}
	seens := make(map[UnionPair]bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for K := index.cosineLSHParam.k; K >= 0; K-- {
			for rp := range index.probe(Hs, K, done) {
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

func (index *CosineLSH) probe(Hs [][]string, prefixSize int, done <-chan struct{}) <-chan UnionPair {
	log.Printf("prefix is now %d", prefixSize)
	out := make(chan UnionPair)
	var wg sync.WaitGroup
	wg.Add(index.cosineLSHParam.l * len(Hs))
	go func() {
		for i := 0; i < index.cosineLSHParam.l; i++ {
			for p := 0; p < len(Hs); p++ {
				go func(i, p int) {
					hk := Hs[p][i]
					ht := index.tables[i]
					defer wg.Done()
					k := sort.Search(len(ht), func(x int) bool {
						return ht[x].hashKey[:prefixSize] >= hk[:prefixSize]
					})
					if k < len(ht) && ht[k].hashKey[:prefixSize] == hk[:prefixSize] {
						for j := k; j < len(ht) && ht[j].hashKey[:prefixSize] == hk[:prefixSize]; j++ {
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
