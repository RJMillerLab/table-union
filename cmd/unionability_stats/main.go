package main

import (
	"log"

	. "github.com/RJMillerLab/table-union/opendata"
)

type pair struct {
	t1name string
	t2name string
}

func main() {
	CheckEnv()
	//ComputeTableUnionabilityVariousC()
	//ComputeAttUnionabilityCDF(100)
	//ComputeAllAttUnionabilityCDF(5000)
	ComputeTableUnionabilityCDF(500)
	//SavePercentileAttUnionability()
	/*
		tstart := GetNow()
		astart := tstart
		queryFilenames := StreamQueryFilenames()
		candidateFilenames := StreamQueryFilenames()
		attProgress := make(chan ProgressCounter)
		tableProgress := make(chan ProgressCounter)
		allAttUnions := make(chan []AttributeUnion, 500)
		allTableUnions := make(chan TableUnion, 500)
		tablePairs := make(chan pair, 500)
		//seen := make(map[string]bool)
		queryTables := make([]string, 0)
		candTables := make([]string, 0)
		for query := range queryFilenames {
			if strings.Contains(query, "data.gov.uk.jsonl") || strings.Contains(query, "open.canada.ca_data_en.jsonl") {
				queryTables = append(queryTables, query)
			}
		}
		for cand := range candidateFilenames {
			if strings.Contains(cand, "data.gov.uk.jsonl") || strings.Contains(cand, "open.canada.ca_data_en.jsonl") {
				candTables = append(candTables, cand)
			}
		}
		log.Printf("len(queryTables): %d", len(queryTables))
		log.Printf("len(candTables): %d", len(candTables))
		swg := &sync.WaitGroup{}
		swg.Add(5)
		go func() {
			for _, query := range queryTables {
				for _, cand := range candTables {
					if query < cand {
						p := pair{
							t1name: query,
							t2name: cand,
						}
						tablePairs <- p
					}
				}
			}
			close(tablePairs)
			swg.Done()
		}()
		log.Printf("started computing att unionability.")
		go func() {
			wg := &sync.WaitGroup{}
			for i := 0; i < 95; i++ {
				wg.Add(1)
				go func() {
					for p := range tablePairs {
						query := p.t1name
						cand := p.t2name
						//attUnions, qColNum, cColNm := ComputeAttUnionabilityScores(query, cand)
						attUnions := ComputeAllAttUnionabilityScores(query, cand)
						allAttUnions <- attUnions
						//tableUnion := ComputeTableUnionability(query, cand, attUnions, qColNum, cColNm)
						//allTableUnions <- tableUnion
					}
					wg.Done()
				}()
			}
			wg.Wait()
			close(allAttUnions)
			close(allTableUnions)
		}()
		go func() {
			DoSaveAttScores(allAttUnions, attProgress)
			close(attProgress)
			swg.Done()
		}()
		go func() {
			//DoSaveTableScores(allTableUnions, tableProgress)
			close(tableProgress)
			swg.Done()
		}()
		go func() {
			total := ProgressCounter{}
			for n := range attProgress {
				total.Values += n.Values
				now := GetNow()
				if total.Values%100 == 0 {
					log.Printf("Processed %d attributes in %.2f seconds\n", total.Values, now-astart)
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
					log.Printf("Processed %d tables in %.2f seconds\n", total.Values, now-tstart)
				}
			}
			swg.Done()
		}()
		swg.Wait()
	*/
	log.Printf("Done collecting stats.")
}
