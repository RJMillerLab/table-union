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
	flag.StringVar(&groundtruthFile, "groundtruthfile", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/groundtruth.json", "groundtruthFile")
	flag.StringVar(&annotationDir, "annotationdir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/annotations", "annotationDir")
	//
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
