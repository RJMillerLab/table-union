package embserver

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/deckarep/golang-set"
	minhashlsh "github.com/ekzhu/minhash-lsh"
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

func getTablePath(tableDir, tableID string) string {
	return filepath.Join(tableDir, tableID)
}

func getDomainValues(domainDir, tableID string, columnIndex int) ([]string, error) {
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

func getDomainEntities(domainDir, tableID string, columnIndex int) ([]string, error) {
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

func getSumEmbVec(domainDir, tableID string, columnIndex int) ([]float64, error) {
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

func jaccard(dom1, dom2 []string) float64 {
	d1set := convertSliceToSet(dom1)
	d2set := convertSliceToSet(dom2)
	d1andd2 := d1set.Intersect(d2set).Cardinality()
	d1ord2 := d1set.Union(d2set).Cardinality()
	return float64(d1andd2) / float64(d1ord2)
}

func containment(dom1, dom2 []string) float64 {
	d1set := convertSliceToSet(dom1)
	d2set := convertSliceToSet(dom2)
	d1andd2 := d1set.Intersect(d2set).Cardinality()
	return float64(d1andd2) / float64(d1set.Cardinality())
}

/*
func readMinhashFromDisk(domainDir, tableID string, columnIndex int) ([]uint64, error) {
	p := filepath.Join(domainDir, tableID, fmt.Sprintf("%d.minhash", columnIndex))
	log.Printf(p)
	return ReadMinhashFromDisk(p, ByteOrder)
}

func ReadMinhashFromDisk(filename string, order binary.ByteOrder) ([]uint64, error) {
	filename = "/home/fnargesian/TABLE_UNION_OUTPUT/domains/open.canada.ca_data_en.jsonl/2af37f1e-56cd-4a1b-8a73-24e22b4ff113/3b5e957d-1244-4fba-8cab-3ddf4ba70806____0/01150022-eng.csv/5.minhash"
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("here1")
		return nil, err
	}
	defer file.Close()
	stats, serr := file.Stat()
	if serr != nil {
		return nil, serr
	}
	var size int64 = stats.Size()
	binVec := make([]byte, size)
	if _, rerr := file.Read(binVec); rerr != nil {
		return nil, rerr
	}
	vec, verr := BytesToIntVec(binVec, order)
	return vec, verr
}

func BytesToIntVec(data []byte, order binary.ByteOrder) ([]uint64, error) {
	size := len(data) / 8
	vec := make([]uint64, size)
	buf := bytes.NewReader(data)
	var v uint64
	for i := range vec {
		if err := binary.Read(buf, order, &v); err != nil {
			return nil, err
		}
		vec[i] = v
	}
	return vec, nil
}
*/

func getDomainName(TableId string, columnIndex int) string {
	return fmt.Sprintf("%s:%d", TableId, columnIndex)
}

func getDomainMinhash(domainDir, TableId string, columnIndex, seed, numHash int) *minhashlsh.Minhash {
	values, err := getDomainValues(domainDir, TableId, columnIndex)
	if err != nil {
		panic(err)
	}
	return getMinhash(values, seed, numHash)
}

func getMinhash(values []string, seed, numHash int) *minhashlsh.Minhash {
	mh := minhashlsh.NewMinhash(seed, numHash)
	for _, value := range values {
		mh.Push([]byte(value))
	}
	return mh
}

func getTableColumnIndexFromDomainName(domainName string) (string, int) {
	ps := strings.Split(domainName, ":")
	tableId := ps[0]
	columnIndex, err := strconv.Atoi(ps[1])
	if err != nil {
		panic(err)
	}
	return tableId, columnIndex
}

func readMinhashSignature(domainDir, tableID string, columnIndex int, numHash int) ([]uint64, error) {
	filename := filepath.Join(domainDir, tableID, fmt.Sprintf("%d.minhash", columnIndex))
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	signature := make([]uint64, numHash)
	for i := range signature {
		if err := binary.Read(f, binary.BigEndian, &(signature[i])); err != nil {
			return nil, err
		}
	}
	return signature, nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
