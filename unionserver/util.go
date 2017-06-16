package unionserver

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
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

func parseFilename(domainDir, filename string) (tableID string, columnIndex int) {
	tableID = strings.TrimPrefix(filepath.Dir(filename), domainDir)
	columnIndex, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename)))
	if err != nil {
		panic(err)
	}
	return
}

func toColumnID(tableID string, columnIndex int) (columnID string) {
	columnID = fmt.Sprintf("%s:%d", tableID, columnIndex)
	return
}

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

func getEmbFilename(tableID, domainDir string, index int) string {
	fullpath := path.Join(domainDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ft-sum"))
	return fullpath
}

func getTextDomains(file, domainDir string) (indices []int) {
	typesFile := path.Join(domainDir, file, "types")
	f, err := os.Open(typesFile)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) == 2 {
			index, err := strconv.Atoi(parts[0])
			if err != nil {
				panic(err)
			}
			if parts[1] == "text" {
				indices = append(indices, index)
			}
		}
	}

	return
}

func getHeaders(file, domainDir string) (headers []string) {
	typesFile := path.Join(domainDir, file, "index")
	f, err := os.Open(typesFile)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		headers = append(headers, scanner.Text())
	}
	return
}

// Classifies an array of strings.  The most dominant choice
// is the class reported.
func classifyValues(values []string) string {
	var counts = make(map[string]int)

	for _, value := range values {
		var key string
		switch {
		case isNumeric(value):
			key = "numeric"
		case isText(value):
			key = "text"
		}
		if key != "" {
			counts[key] += 1
		}
	}

	var (
		maxKey   string
		maxCount int
	)
	for k, v := range counts {
		if v > maxCount {
			maxKey = k
		}
	}
	return maxKey
}

var (
	patternInteger *regexp.Regexp
	patternFloat   *regexp.Regexp
	patternWord    *regexp.Regexp
)

func init() {
	patternInteger = regexp.MustCompile(`^\d+$`)
	patternFloat = regexp.MustCompile(`^\d+\.\d+$`)
	patternWord = regexp.MustCompile(`[[:alpha:]]{2,}`)
}

func isNumeric(val string) bool {
	return patternInteger.MatchString(val) || patternFloat.MatchString(val)
}

func isText(val string) bool {
	return patternWord.MatchString(val)
}
