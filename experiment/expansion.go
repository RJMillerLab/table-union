package experiment

import (
	"encoding/csv"
	"os"
	"strings"

	"github.com/ekzhu/counter"
	"github.com/ekzhu/datatable"
)

// Expansion is a measure for the evaluating the addition of values in
// a table union
type Expansion struct {
	ColumnExpansions []int
	RowExpansion     int
}

// SolumnExpansion is a measure for evaluating the expansion incurred
// by unioning two columns (set operation).
type ColumnExpansion struct {
	Col1NumUniqueValues  int
	Col2NumUniqueValues  int
	NumUniqueValuesAdded int
}

// ComputeExpansion computes the expansion measure given two CSV table files
// and mapping from t1 column indexes to t2 column indexes
func ComputeExpansion(t1Filename, t2Filename string, matches map[int]int) Expansion {
	t1File, err := os.Open(t1Filename)
	if err != nil {
		panic(err)
	}
	defer t1File.Close()
	t1, err := datatable.FromCSV(csv.NewReader(t1File))
	columnCounters := make([]*counter.Counter, t1.NumCol())
	for i := range columnCounters {
		columnCounters[i] = counter.NewCounter()
	}
	rowCounter := counter.NewCounter()
	// Count the current table
	for i := 0; i < t1.NumRow(); i++ {
		rowCounter.Update(strings.Join(t1.GetRow(i), ","))
	}
	for j := 0; j < t1.NumCol(); j++ {
		t1.ApplyColumn(func(x int, v string) error {
			columnCounters[j].Update(v)
			return nil
		}, j)
	}
	// Check current unique counts
	numUniqueRows := rowCounter.Unique()
	numUniqueValues := make([]int, len(columnCounters))
	for j := range columnCounters {
		numUniqueValues[j] = columnCounters[j].Unique()
	}
	// Count the new table
	t2File, err := os.Open(t2Filename)
	if err != nil {
		panic(err)
	}
	defer t2File.Close()
	t2, err := datatable.FromCSV(csv.NewReader(t2File))
	t1.Merge(t2, matches)
	for i := 0; i < t1.NumRow(); i++ {
		rowCounter.Update(strings.Join(t1.GetRow(i), ","))
	}
	for j := 0; j < t1.NumCol(); j++ {
		t1.ApplyColumn(func(x int, v string) error {
			columnCounters[j].Update(v)
			return nil
		}, j)
	}
	// Check expansion
	numNewUniqueRows := rowCounter.Unique() - numUniqueRows
	numNewUniqueValues := make([]int, len(columnCounters))
	for j := range columnCounters {
		numNewUniqueValues[j] = columnCounters[j].Unique() - numUniqueValues[j]
	}
	return Expansion{
		ColumnExpansions: numNewUniqueValues,
		RowExpansion:     numNewUniqueRows,
	}
}

// ComputeColumnExpansion computes the expansion measure given two colum indexes of
// of two csv files.
func ComputeColumnExpansion(t1Filename, t2Filename string, t1ColumnIndex, t2ColumnIndex int) ColumnExpansion {
	//read the first table
	t1File, err := os.Open(t1Filename)
	if err != nil {
		panic(err)
	}
	defer t1File.Close()
	t1, err := datatable.FromCSV(csv.NewReader(t1File))
	t1ColumnCounter := counter.NewCounter()
	t1.ApplyColumn(func(x int, v string) error {
		t1ColumnCounter.Update(v)
		return nil
	}, t1ColumnIndex)
	t1NumUniqueValues := t1ColumnCounter.Unique()
	//read the second table
	t2File, err := os.Open(t2Filename)
	if err != nil {
		panic(err)
	}
	defer t2File.Close()
	t2, err := datatable.FromCSV(csv.NewReader(t2File))
	t2ColumnCounter := counter.NewCounter()
	t2.ApplyColumn(func(x int, v string) error {
		t2ColumnCounter.Update(v)
		return nil
	}, t2ColumnIndex)
	t2NumUniqueValues := t2ColumnCounter.Unique()
	// Count the union table
	matches := make(map[int]int)
	matches[t1ColumnIndex] = t2ColumnIndex
	t1.Merge(t2, matches)
	t1.ApplyColumn(func(x int, v string) error {
		t1ColumnCounter.Update(v)
		return nil
	}, t1ColumnIndex)
	unionUniqueValues := t1ColumnCounter.Unique() - t1NumUniqueValues
	return ColumnExpansion{
		Col1NumUniqueValues:  t1NumUniqueValues,
		Col2NumUniqueValues:  t2NumUniqueValues,
		NumUniqueValuesAdded: unionUniqueValues,
	}
}
