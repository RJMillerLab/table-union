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
	CandTableID   string
	CandColIndex  int
	QueryColIndex int
	Sim           float64
}
