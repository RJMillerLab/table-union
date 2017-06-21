package unionserver

type embDomain struct {
	columnIndex int
	sumVec      []float64
}

type edge struct {
	srcIndex  int
	destIndex int
}

type Pair struct {
	CandTableID   string
	CandColIndex  int
	QueryColIndex int
	Sim           float64
}

/*
func Align(candTableID, domainDir string, query [][]float64, K int) Union {
	queryDomains := make([]embDomain, 0)
	candDomains := make([]embDomain, 0)
	for _, index := range getTextDomains(candTableID, domainDir) {
		embFilename := getEmbFilename(candTableID, domainDir, index)
		if _, err := os.Stat(embFilename); os.IsNotExist(err) {
			log.Printf("Embedding file %s does not exist.", embFilename)
			continue
		}
		vec, err := embedding.ReadVecFromDisk(embFilename, ByteOrder)
		if err != nil {
			log.Printf("Error in reading %s from disk.", embFilename)
			continue
		}
		ce := embDomain{
			columnIndex: index,
			sumVec:      vec,
		}
		candDomains = append(candDomains, ce)
	}
	for index, vec := range query {
		ce := embDomain{
			columnIndex: index,
			sumVec:      vec,
		}
		queryDomains = append(queryDomains, ce)
	}
	// computing edge weights and sorting
	queue := pqueue.NewTopKQueue(len(queryDomains) * len(candDomains))
	for _, qd := range queryDomains {
		for _, cd := range candDomains {
			cosine := embedding.Cosine(cd.sumVec, qd.sumVec)
			e := edge{
				srcIndex:  qd.columnIndex,
				destIndex: cd.columnIndex,
			}
			queue.Push(e, -1*cosine)
		}
	}
	// greedy alignment
	//Union alignment
	source := make(map[int]map[int]float64)
	dest := make(map[int]int)
	var kUnionability float64
	for i := queue.Size() - 1; i >= 0; i-- {
		m, s := queue.Pop()
		e := m.(edge)
		if _, ok := source[e.srcIndex]; !ok {
			if _, ok := dest[e.destIndex]; !ok {
				p := make(map[int]float64)
				p[e.destIndex] = -1 * s
				source[e.srcIndex] = p
				dest[e.destIndex] = e.srcIndex
				kUnionability = -1 * s
				if len(source) == K {
					u := Union{
						CandTableID:  candTableID,
						CandHeader:   getHeaders(candTableID, domainDir),
						Alignment:    source,
						Kunioability: kUnionability,
					}
					return u
				}
			}
		}
	}
	log.Printf("Did not find k-unionability")
	u := Union{
		CandTableID:  candTableID,
		CandHeader:   getHeaders(candTableID, domainDir),
		Alignment:    source,
		Kunioability: kUnionability,
	}
	return u
}

func AlignEasy(queue *pqueue.TopKQueue, domainDir string, K int) Union {
	source := make(map[int]map[int]float64)
	dest := make(map[int]int)
	for i := 0; i < queue.Size(); i++ {
		pair, score := queue.Pop()
		kUnionability := -1.0
		if i == 0 {
			kUnionability = score
		}
		queryColIndex := pair.(Pair).QueryColIndex
		candColIndex := pair.(Pair).CandColIndex
		if _, ok := source[queryColIndex]; !ok {
			if _, ok := dest[candColIndex]; !ok {
				p := make(map[int]float64)
				p[candColIndex] = score
				source[queryColIndex] = p
				dest[candColIndex] = queryColIndex
				if len(source) == K {
					u := Union{
						CandTableID:  pair.(Pair).CandTableID,
						CandHeader:   getHeaders(pair.(Pair).CandTableID, domainDir),
						Alignment:    source,
						Kunioability: kUnionability,
					}
					return u
				}
			}
		}
	}
	log.Printf("Did not find k-unionability")
	u := Union{
		Kunioability: -1.0,
	}
	return u
}

func AlignTooEasy(queue *pqueue.TopKQueue, domainDir string) Union {
	source := make(map[int]map[int]float64)
	var candTableID string
	var kUnionability float64
	K := queue.Size()
	for i := 0; i < K; i++ {
		pair, score := queue.Pop()
		kUnionability = -1 * score
		candTableID = pair.(Pair).CandTableID
		queryColIndex := pair.(Pair).QueryColIndex
		candColIndex := pair.(Pair).CandColIndex
		p := make(map[int]float64)
		p[candColIndex] = -1 * score
		source[queryColIndex] = p
	}
	u := Union{
		CandTableID:  candTableID,
		CandHeader:   getHeaders(candTableID, domainDir),
		Alignment:    source,
		Kunioability: kUnionability,
	}
	return u
}
*/
