package embserver

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode"
)

var (
	DefaultTransFun = func(s string) string {
		return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
	}
	AdvancedTransFun = func(s string) string {
		return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
	}
	DefaultTokenFun = func(s string) []string { return strings.Split(s, " ") }
)

func fromColumnID(columnID string) (tableID string, columnIndex int) {
	items := strings.Split(columnID, ":")
	if len(items) != 2 {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	tableID = items[0]
	var err error
	columnIndex, err = strconv.Atoi(items[1])
	if err != nil {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	return
}

func toColumnID(tableID string, columnIndex int) (columnID string) {
	columnID = fmt.Sprintf("%s:%d", tableID, columnIndex)
	return
}

func dotProduct(x, y []float64) float64 {
	if len(x) != len(y) {
		panic("Length of vectors not equal")
	}
	p := 0.0
	for i := range x {
		p += x[i] * y[i]
	}
	return p
}

//func filenameToColumnID(filename string) (columnID string) {
//	tableID := strings.strings.Replace(filename, OutputDir, "", -1)
//	columnID = fmt.Sprintf("%s:%d", tableID, columnIndex)
//	return
//}

func filenameToColumnID(filename string) string {
	tableID := strings.Replace(path.Dir(filename), OutputDir+"/domains/", "", -1)
	columnIndex := strings.Replace(path.Base(filename), path.Ext(filename), "", -1)
	columnID := fmt.Sprintf("%s:%s", tableID, columnIndex)
	return columnID
}

func getTablePath(tableID string) string {
	path := path.Join("/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/", tableID)
	return path
}
