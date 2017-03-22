package search

type VecEntry struct {
	TableID     int       `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Embedding   []float64 `json:"embedding"`
}

type SearchIndex struct {
}
