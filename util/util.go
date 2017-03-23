package util

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

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

func DumpJson(file string, v interface{}) error {
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

func MkDomain(col []string) []string {
	found := make(map[string]bool)
	var dom []string
	for _, s := range col {
		if !found[strings.ToLower(s)] && IsNull(strings.ToLower(s)) != true {
			found[strings.ToLower(s)] = true
			dom = append(dom, s)
		}
	}
	return dom
}
