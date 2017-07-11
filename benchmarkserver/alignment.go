package benchmarkserver

type embDomain struct {
	columnIndex int
	sumVec      []float64
}

type edge struct {
	srcIndex  int
	destIndex int
}

type Pair struct {
	CandTableID            string
	CandColIndex           int
	QueryColIndex          int
	Jaccard                float64
	Hypergeometric         float64
	OntologyJaccard        float64
	OntologyHypergeometric float64
	Cosine                 float64
	F                      float64
	T2                     float64
	Sim                    float64
}
