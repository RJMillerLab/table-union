package main

import (
	. "github.com/RJMillerLab/table-union/opendata"
)

type pair struct {
	t1name string
	t2name string
}

func main() {
	CheckEnv()
	//ComputeTableUnionabilityVariousC()
	ComputeAttUnionabilityPercentile(100)
	ComputeTableUnionabilityPercentile(100)
	/*
		tstart := GetNow()
		astart := tstart
		queryFilenames := StreamQueryFilenames()
		attProgress := make(chan ProgressCounter)
		tableProgress := make(chan ProgressCounter)
		allAttUnions := make(chan []AttributeUnion, 500)
		allTableUnions := make(chan TableUnion, 500)
		tablePairs := make(chan pair, 500)
		seen := make(map[string]bool)
		go func() {
			for query := range queryFilenames {
				if len(seen) >= 5000 {
					continue
				}
				candFilenames := StreamQueryFilenames()
				for cand := range candFilenames {
					if _, ok := seen[query+" "+cand]; !ok {
						if _, ok := seen[cand+" "+query]; !ok {
							seen[cand+" "+query] = true
							seen[query+" "+cand] = true
							tablePairs <- pair{
								t1name: query,
								t2name: cand,
							}
						}
					}
				}
			}
			close(tablePairs)
		}()
		go func() {
			wg := &sync.WaitGroup{}
			for i := 0; i < 35; i++ {
				wg.Add(1)
				go func() {
					for p := range tablePairs {
						query := p.t1name
						cand := p.t2name
						attUnions, qColNum, cColNm := ComputeAttUnionabilityScores(query, cand)
						allAttUnions <- attUnions
						tableUnion := ComputeTableUnionability(query, cand, attUnions, qColNum, cColNm)
						allTableUnions <- tableUnion
					}
					wg.Done()
				}()
			}
			wg.Wait()
			close(allAttUnions)
			close(allTableUnions)
		}()
		swg := &sync.WaitGroup{}
		swg.Add(4)
		go func() {
			//for attUnions := range allAttUnions {
			DoSaveAttScores(allAttUnions, attProgress)
			//}
			close(attProgress)
			swg.Done()
		}()
		go func() {
			//for tableUnion := range allTableUnions {
			DoSaveTableScores(allTableUnions, tableProgress)
			//}
			close(tableProgress)
			swg.Done()
		}()
		go func() {
			total := ProgressCounter{}
			for n := range attProgress {
				total.Values += n.Values
				now := GetNow()
				if total.Values%100 == 0 {
					fmt.Printf("Processed %d attributes in %.2f seconds\n", total.Values, now-astart)
				}
			}
			swg.Done()
		}()
		go func() {
			total := ProgressCounter{}
			for n := range tableProgress {
				total.Values += n.Values
				now := GetNow()
				if total.Values%100 == 0 {
					fmt.Printf("Processed %d tables in %.2f seconds\n", total.Values, now-tstart)
				}
			}
			swg.Done()
		}()
		swg.Wait()

		log.Printf("Done collecting stats.")
	*/
}
