package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	var groundtruthFile string
	var annotationDir string
	var testDir string
	var rawtestDir string
	flag.StringVar(&groundtruthFile, "groundtruthfile", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/groundtruth.json", "groundtruthFile")
	flag.StringVar(&annotationDir, "annotationdir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/annotations", "annotationDir")
	flag.StringVar(&testDir, "testdir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/testdata", "test data directory")
	flag.StringVar(&rawtestDir, "rawtestdir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/rawcolumns", "raw test data directory")
	//
	columnMap := generate_groundtruth(groundtruthFile, annotationDir)
	//
	generate_testdata(rawtestDir, testDir, columnMap)
}

func generate_testdata(rawtestDir, testDir string, columnMap map[string]string) {
	colFiles := loadDirectory(rawtestDir)
	for icf, cf := range colFiles {
		if icf%100 == 0 {
			log.Printf("processed %d files.", icf)
		}
		f, err := os.Open(cf)
		if err != nil {
			fmt.Printf("column file panic")
			panic(err.Error())
		}
		of, err := os.Create(testDir + "/" + columnMap[strings.ToLower(strings.Replace(cf, rawtestDir+"/", "", -1))])
		if err != nil {
			fmt.Printf("error in creating a file")
			panic(err.Error())
		}
		scanner := bufio.NewScanner(f)
		w := bufio.NewWriter(of)
		_, err = w.WriteString("0\n")
		_, err = w.WriteString("false\n")
		if err != nil {
			fmt.Printf("error in writing")
			panic(err.Error())
		}
		values := make(map[string]bool)
		for scanner.Scan() {
			a := strings.Replace(strings.Replace(strings.Replace(strings.Replace(strings.ToLower(scanner.Text()), "_", " ", -1), "(", "", -1), ")", "", -1), ",", "", -1)
			if a != "" {
				if _, ok := values[a]; !ok {
					_, err := w.WriteString(a + "\n")
					if err != nil {
						fmt.Printf("error in writing")
						panic(err.Error())
					}
					values[a] = true
				}
			}
		}
		w.Flush()
	}
}

func generate_groundtruth(groundtruthFile, annotationDir string) map[string]string {
	classcolumn := make(map[string][]string)
	columnclass := make(map[string][]string)
	columns := make(map[string]string)
	fcounter := 0
	aFiles := loadDirectory(annotationDir)
	for _, af := range aFiles {
		f, err := os.Open(af)
		if err != nil {
			fmt.Printf("aFile panic")
			panic(err.Error())
		}
		colname := strings.ToLower(strings.Replace(af, annotationDir+"/", "", -1))
		if _, ok := columns[colname]; !ok {
			columns[colname] = strconv.Itoa(fcounter)
			fcounter += 1
		}
		scanner := bufio.NewScanner(f)
		var anns []string
		for scanner.Scan() {
			a := strings.ToLower(scanner.Text())
			if a != "wordnet_entity_100001740" {
				anns = append(anns, a)
				_, ok := classcolumn[a]
				if ok {
					//classcolumn[a] = append(classcolumn[a], colname)
					classcolumn[a] = append(classcolumn[a], columns[colname])
				} else {
					var cs []string
					cs = append(cs, columns[colname])
					classcolumn[a] = cs
				}
			}
		}
		f.Close()
		if len(anns) > 0 {
			columnclass[columns[colname]] = anns
		}
	}
	sum := 0
	cmin := 8000
	cmax := 0
	groundtruth := make(map[string][]string)
	for col, cs := range columnclass {
		var cands []string
		for _, c := range cs {
			//if c != "wordnet_entity_100001740" {
			for _, d := range classcolumn[c] {
				cands = append(cands, d)
			}
			//}
		}
		if len(cands) > 0 {
			sum += len(cands)
			groundtruth[col] = cands
			if len(cands) < cmin {
				cmin = len(cands)
			}
			if len(cands) > cmax {
				cmax = len(cands)
			}
		}
	}
	log.Printf("total number of columns %d", len(columnclass))
	log.Printf("total number of columns with matches %d", len(groundtruth))
	log.Printf("max number of matches: %d", cmax)
	log.Printf("min number of matches: %d", cmin)
	log.Printf("avg number of matches: %f", float64(sum)/float64(len(groundtruth)))
	log.Printf("len column maps: %d", len(columns))
	//
	dumpJson(groundtruthFile, groundtruth)
	return columns
}

func loadDirectory(dirname string) (filenames []string) {
	dir, err := os.Open(dirname)
	defer dir.Close()
	if err != nil {
		panic(err.Error())
	}

	fi_list, err := dir.Readdir(-1)
	if err != nil {
		panic(err.Error())
	}

	for _, fi := range fi_list {
		if !fi.IsDir() {
			filenames = append(filenames, filepath.Join(dirname, fi.Name()))
		}
	}

	return filenames
}

func dumpJson(file string, v interface{}) {
	buffer, err := json.Marshal(v)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(file, buffer, 0777)
	if err != nil {
		panic(err.Error())
	}
}
