package wwt

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/RJMillerLab/table-union/embedding"
	fasttext "github.com/ekzhu/go-fasttext"
)

func writeToFile(file string, lines []string) {
	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	for _, l := range lines {
		if _, err := w.WriteString(l); err != nil {
			panic(err)
		}
		if _, err := w.WriteString("\n"); err != nil {
			panic(err)
		}
	}
	w.Flush()
	f.Close()
}

func xmlFiles(dir string) chan string {
	files := make(chan string)
	go func() {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if strings.HasSuffix(info.Name(), ".xml") {
				files <- path
			}
			return nil
		})
		close(files)
	}()
	return files
}

type TableAnnotation struct {
	XMLName         xml.Name          `xml:"tableAnnotations"`
	ColAnnotations  ColumnAnnotations `xml:"columnAnnotations"`
	CellAnnotations CellAnnotations   `xml:"cellAnnotatoons"`
}

type ColumnAnnotations struct {
	XMLName  xml.Name   `xml:"columnAnnotations"`
	ColAnnos []ColAnnos `xml:"colAnnos"`
}

type ColAnnos struct {
	XMLName xml.Name `xml:"colAnnos"`
	Annos   []Anno   `xml:"anno"`
	Col     string   `xml:"col,attr"`
}

type Anno struct {
	XMLName xml.Name `xml:"anno"`
	Name    string   `xml:"name,attr"`
	Value   string   `xml:"value,attr"`
}

type CellAnnotations struct {
	XMLName xml.Name `xml:"cellAnnotatoons"` // the original xml file has a typo
	Rows    []Row    `xml:"row"`
}

type Row struct {
	XMLName  xml.Name `xml:"row"`
	Entities []string `xml:"entity"`
}

type WWTColumn struct {
	ColumnName  string
	Column      []string
	Annotations []string
	Vec         []float64
}

type WWT struct {
	dir      string
	ft       *fasttext.FastText
	transFun func(string) string
	tokenFun func(string) []string
}

func NewWWT(dir string, ft *fasttext.FastText) *WWT {
	return &WWT{
		dir: dir,
		ft:  ft,
		transFun: func(s string) string {
			s = strings.Replace(strings.Replace(s, "(", "", -1), ")", "", -1)
			return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
		},
		tokenFun: func(s string) []string {
			return strings.Split(s, "_")
		},
	}
}

func (w *WWT) ReadColumns() <-chan *WWTColumn {
	out := make(chan *WWTColumn)
	go func() {
		for col := range readRaw(w.dir) {
			vec, err := embedding.GetDomainEmbPCA(w.ft, w.tokenFun, w.transFun, col.Column)
			if err != nil {
				log.Printf("Error in column %s: %s", col.ColumnName, err)
			}
			col.Vec = vec
			out <- col
		}
		close(out)
	}()
	return out
}

func readRaw(wwtDir string) <-chan *WWTColumn {
	out := make(chan *WWTColumn)
	go func() {
		for file := range xmlFiles(wwtDir) {
			b, err := ioutil.ReadFile(file)
			if err != nil {
				panic(err)
			}
			var tableAnnos TableAnnotation
			err = xml.Unmarshal(b, &tableAnnos)
			if err != nil {
				panic(err)
			}
			// Indices of columns that have annotations
			colIndices := make([]int, len(tableAnnos.ColAnnotations.ColAnnos))
			// Each column has a list of annotations
			annotations := make([][]string, len(tableAnnos.ColAnnotations.ColAnnos))
			for i, colAnnos := range tableAnnos.ColAnnotations.ColAnnos {
				colIndices[i], err = strconv.Atoi(colAnnos.Col)
				if err != nil {
					panic(err)
				}
				annotations[i] = make([]string, len(colAnnos.Annos))
				for j, anno := range colAnnos.Annos {
					annotations[i][j] = anno.Name

				}
			}
			// Columns each with a list of row values
			columns := make([][]string, len(colIndices))
			for i := range columns {
				columns[i] = make([]string, len(tableAnnos.CellAnnotations.Rows))
			}
			for i, row := range tableAnnos.CellAnnotations.Rows {
				for j, colIndex := range colIndices {
					columns[j][i] = row.Entities[colIndex]
				}
			}
			for i, colIndex := range colIndices {
				colName := fmt.Sprintf("%s_%d", filepath.Base(file), colIndex)
				out <- &WWTColumn{
					ColumnName:  colName,
					Column:      columns[i],
					Annotations: annotations[i],
				}
			}
		}
		close(out)
	}()
	return out
}
