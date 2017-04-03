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
	//generate_groundtruth(groundtruthFile, annotationDir)
	//
	generate_testdata(rawtestDir, testDir)
}

func generate_testdata(rawtestDir, testDir string) {
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
		of, err := os.Create(strings.Replace(cf, rawtestDir, testDir, -1))
		if err != nil {
			fmt.Printf("error in creating a file")
			panic(err.Error())
		}
		scanner := bufio.NewScanner(f)
		w := bufio.NewWriter(of)
		fname := strings.Replace(cf, rawtestDir+"/", "", -1)
		_, err = w.WriteString(fname + "\n")
		if err != nil {
			fmt.Printf("error in writing")
			panic(err.Error())
		}
		for scanner.Scan() {
			a := strings.Replace(strings.Replace(strings.Replace(strings.ToLower(scanner.Text()), "_", " ", -1), "(", "", -1), ")", "", -1)
			if a != "" {
				_, err := w.WriteString(a + "\n")
				if err != nil {
					fmt.Printf("error in writing")
					panic(err.Error())
				}
			}
		}
		w.Flush()
	}
}

func generate_groundtruth(groundtruthFile, annotationDir string) {
	classcolumn := make(map[string][]string)
	columnclass := make(map[string][]string)
	aFiles := loadDirectory(annotationDir)
	for _, af := range aFiles {
		f, err := os.Open(af)
		if err != nil {
			fmt.Printf("aFile panic")
			panic(err.Error())
		}
		colname := strings.ToLower(strings.Replace(af, annotationDir+"/", "", -1))
		scanner := bufio.NewScanner(f)
		var anns []string
		for scanner.Scan() {
			a := strings.ToLower(scanner.Text())
			anns = append(anns, a)
			_, ok := classcolumn[a]
			if ok {
				classcolumn[a] = append(classcolumn[a], colname)
			} else {
				var cs []string
				cs = append(cs, colname)
				classcolumn[a] = cs
			}
		}
		f.Close()
		if len(anns) > 0 {
			columnclass[colname] = anns
		}
	}
	sum := 0
	cmin := 8000
	cmax := 0
	groundtruth := make(map[string][]string)
	for col, cs := range columnclass {
		var cands []string
		for _, c := range cs {
			if c != "wordnet_entity_100001740" {
				for _, d := range classcolumn[c] {
					cands = append(cands, d)
				}
			}
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
	//
	dumpJson(groundtruthFile, groundtruth)
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
