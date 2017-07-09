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
	CandTableID     string
	CandColIndex    int
	QueryColIndex   int
	Jaccard         float64
	JaccardProb     float64
	OntologyJaccard float64
	OntologyProb    float64
	Cosine          float64
	CosineProb      float64
	F               float64
	T2              float64
	Sim             float64
}
