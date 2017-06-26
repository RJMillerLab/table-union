package benchmarkserver

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

	minhashlsh "github.com/RJMillerLab/table-union/minhashlsh"
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
	seed            = 1
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

func getMinhashFilename(tableID, domainDir string, index int) string {
	fullpath := path.Join(domainDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "minhash"))
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

func getTextHeaders(file, domainDir string) []string {
	textHeaderIndices := getTextDomains(file, domainDir)
	headers := getHeaders(file, domainDir)
	textHeaders := make([]string, 0)
	for _, i := range textHeaderIndices {
		textHeaders = append(textHeaders, headers[i])
	}
	return textHeaders
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

func jaccard(dom1, dom2 []string) float64 {
	d1set := convertSliceToSet(dom1)
	d2set := convertSliceToSet(dom2)
	d1andd2 := d1set.Intersect(d2set).Cardinality()
	d1ord2 := d1set.Union(d2set).Cardinality()
	return float64(d1andd2) / float64(d1ord2)
}

func estimatedJaccard(query, candidate []uint64) float64 {
	intersection := 0
	for i := 0; i < len(query); i++ {
		if query[i] == candidate[i] {
			intersection += 1
		}
	}
	return float64(intersection) / float64(len(query))
}

func convertSliceToSet(slice []string) mapset.Set {
	set := mapset.NewSet()
	for _, v := range slice {
		set.Add(strings.ToLower(v))
	}
	return set
}

func index(a []string, s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}
	return -1
}

func getDomainMinhash(tokenFun func(string) []string, transFun func(string) string, column []string, numHash int) []uint64 {
	values := tokenizedValues(column, tokenFun, transFun)
	mh := minhashlsh.NewMinhash(seed, numHash)

	for tokens := range values {
		for _, word := range tokens {
			mh.Push([]byte(word))
		}
	}
	return mh.Signature()
}

// Produce a channel of values (tokenized)
func tokenizedValues(values []string, tokenFun func(string) []string, transFun func(string) string) chan []string {
	out := make(chan []string)
	go func() {
		for _, v := range values {
			v = transFun(v)
			// Tokenize
			tokens := tokenFun(v)
			if len(tokens) > 5 {
				// Skip text values
				continue
			}
			for i, t := range tokens {
				tokens[i] = transFun(t)
			}
			out <- tokens
		}
		close(out)
	}()
	return out
}
