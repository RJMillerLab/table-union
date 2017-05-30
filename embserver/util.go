package embserver

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/deckarep/golang-set"
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

func parseFilename(domainDir, filename string) (tableID string, columnIndex int) {
	tableID = strings.TrimPrefix(filepath.Dir(filename), domainDir)
	columnIndex, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename)))
	if err != nil {
		panic(err)
	}
	return
}

func GetTablePath(tableDir, tableID string) string {
	return filepath.Join(tableDir, tableID)
}

func GetDomainValues(domainDir, tableID string, columnIndex int) ([]string, error) {
	p := filepath.Join(domainDir, tableID, fmt.Sprintf("%d.values", columnIndex))
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		v := scanner.Text()
		values = append(values, v)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func GetDomainEntities(domainDir, tableID string, columnIndex int) ([]string, error) {
	p := filepath.Join(domainDir, tableID, fmt.Sprintf("%d.entities", columnIndex))
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		v := scanner.Text()
		values = append(values, v)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func GetSumEmbVec(domainDir, tableID string, columnIndex int) ([]float64, error) {
	p := filepath.Join(domainDir, tableID, fmt.Sprintf("%d.ft-sum", columnIndex))
	return embedding.ReadVecFromDisk(p, ByteOrder)

}

func convertSliceToSet(slice []string) mapset.Set {
	set := mapset.NewSet()
	for _, v := range slice {
		set.Add(strings.ToLower(v))
	}
	return set
}

func Jaccard(dom1, dom2 []string) float64 {
	d1set := convertSliceToSet(dom1)
	d2set := convertSliceToSet(dom2)
	d1andd2 := d1set.Intersect(d2set).Cardinality()
	d1ord2 := d1set.Union(d2set).Cardinality()
	return float64(d1andd2) / float64(d1ord2)
}

func Containment(dom1, dom2 []string) float64 {
	d1set := convertSliceToSet(dom1)
	d2set := convertSliceToSet(dom2)
	d1andd2 := d1set.Intersect(d2set).Cardinality()
	return float64(d1andd2) / float64(d1set.Cardinality())
}
