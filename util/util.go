package util

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Tokenizes an attribute
func tokenize(att []string, qgrams map[string]int) ([]int, map[string]int) {
	tokens := make([]int, len(qgrams))
	tmap := make(map[string]int)
	for _, e := range att {
		e = strings.ToLower(strings.TrimSpace(strings.Replace(e, "_", " ", -1)))
		for ix, _ := range e {
			if ix <= len(e)-3 {
				if val, ok := qgrams[e[ix:ix+3]]; ok {
					tokens[val] = tokens[val] + 1
					if _, ok := tmap[e[ix:ix+3]]; ok {
						tmap[e[ix:ix+3]] = tmap[e[ix:ix+3]] + 1
					} else {
						tmap[e[ix:ix+3]] = 1
					}
				}
			}
		}
	}
	return tokens, tmap
}

// Loads a directory
func LoadDirectory(dirname string) (filenames []string) {
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

func LoadJson(file string, v interface{}) error {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buffer, v)
	if err != nil {
		return err
	}
	return nil
}

func dumpJson(file string, v interface{}) error {
	buffer, err := json.Marshal(v)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(file, buffer, 0664)
	if err != nil {
		return err
	}
	return nil
}

func ReadLines(filename string) ([]string, error) {
	var lines []string
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return lines, err

	}
	buf := bytes.NewBuffer(file)
	for {
		line, err := buf.ReadString('\n')
		if len(line) == 0 {
			if err != nil {
				if err == io.EOF {
					break
				}
				return lines, err
			}
		}
		lines = append(lines, line)
		if err != nil && err != io.EOF {
			return lines, err
		}
	}
	return lines, nil
}

func IsNumeric(cell string) bool {
	if _, err := strconv.ParseFloat(cell, 64); err != nil {
		return false
	}
	return true
}
