package opendata

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

func readLines(filename string, maxLines int) (lines []string, err error) {
	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		value := strings.TrimSpace(scanner.Text())
		if value != "" {
			lines = append(lines, scanner.Text())
			if maxLines > 0 && maxLines <= len(lines) {
				break
			}
		}
	}
	return lines, nil
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func StreamFilenames() <-chan string {
	output := make(chan string)

	go func() {
		f, _ := os.Open(opendata_list)
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 3)
			filename := path.Join(parts...)
			output <- filename
		}
		close(output)
	}()

	return output
}

type Domain struct {
	Filename string
	Index    int
	Values   []string
}

func (domain *Domain) Save(logger *log.Logger) int {
	var filepath string
	dirPath := path.Join(output_dir, "domains", domain.Filename)

	if domain.Index < 0 {
		// Encountered a file for the first time.
		// This is the header, so create the index file
		filepath = path.Join(dirPath, "index")
	} else {
		filepath = path.Join(dirPath, fmt.Sprintf("%d.values", domain.Index))
	}

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	defer f.Close()

	if err == nil {
		for _, value := range domain.Values {
			fmt.Fprintln(f, value)
		}
	} else {
		panic(fmt.Sprintf("Unable to save: %s", err.Error()))
	}

	logger.Printf("Written to %s\n", filepath)
	return len(domain.Values)
}

func GetDomainHeader(file string) *Domain {
	filepath := path.Join(output_dir, "domains", file, "index")
	lines, err := readLines(filepath, -1)
	if err != nil {
		panic(err)
	}
	return &Domain{
		Filename: file,
		Index:    -1,
		Values:   lines,
	}
}

func domainsFromCells(cells [][]string, filename string, width int) []*Domain {
	if len(cells) == 0 {
		return nil
	}

	domains := make([]*Domain, width)
	for i := 0; i < width; i++ {
		domains[i] = &Domain{filename, i, nil}
	}
	for _, row := range cells {
		for c := 0; c < width; c++ {
			if c < len(row) {
				value := strings.TrimSpace(row[c])
				if len(value) > 2 {
					domains[c].Values = append(domains[c].Values, row[c])
				}
			}
		}
	}
	return domains
}

func makeDomains(filenames <-chan string, out chan *Domain) {
	for filename := range filenames {
		f, err := os.Open(Filepath(filename))
		if err != nil {
			f.Close()
			continue
		}

		rdr := csv.NewReader(f)
		header, err := rdr.Read()
		if err != nil {
			continue
		}
		width := len(header)

		headerDomain := &Domain{
			Filename: filename,
			Index:    -1,
			Values:   header,
		}

		out <- headerDomain

		var cells [][]string

		for {
			row, err := rdr.Read()
			if err == io.EOF {
				for _, domain := range domainsFromCells(cells, filename, width) {
					out <- domain
				}
				break
			} else {
				cells = append(cells, row)
				if len(cells)*width > 1000000 {
					for _, domain := range domainsFromCells(cells, filename, width) {
						out <- domain
					}
					cells = nil
				}
			}
		}
		f.Close()
	}
}

func StreamDomainsFromFilenames(fanout int, filenames <-chan string) <-chan *Domain {
	out := make(chan *Domain)

	wg := &sync.WaitGroup{}

	for id := 0; id < fanout; id++ {
		wg.Add(1)
		go func(id int) {
			makeDomains(filenames, out)
			wg.Done()
		}(id)
	}
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

type ProgressCounter struct {
	Values int
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func DoSaveDomainValues(fanout int, domains <-chan *Domain) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	queues := make([]chan *Domain, fanout)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		queues[i] = make(chan *Domain)
		wg.Add(1)
		go func(id int, queue chan *Domain) {
			logf, err := os.OpenFile(path.Join(output_dir, "logs", fmt.Sprintf("save_domain_values_%d.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			defer logf.Close()

			if err != nil {
				panic(err)
			}
			logger := log.New(logf, fmt.Sprintf("[%d]", id), log.Lshortfile)

			for domain := range queue {
				n := domain.Save(logger)
				progress <- ProgressCounter{n}
			}
			wg.Done()
		}(i, queues[i])
	}

	go func() {
		for domain := range domains {
			k := hash(domain.Filename) % fanout
			queues[k] <- domain
		}
		for i := 0; i < fanout; i++ {
			close(queues[i])
		}
	}()

	go func() {
		wg.Wait()
		close(progress)
	}()

	return progress
}

// Classifies the values into one of several categories
// "numeric"
// "id"
// "word"
// "phrase"
// "text"

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

func classifyDomains(file string) {
	header := GetDomainHeader(file)
	fout, err := os.OpenFile(path.Join(output_dir, "domains", file, "types"), os.O_CREATE|os.O_WRONLY, 0644)
	defer fout.Close()

	if err != nil {
		panic(err)
	}

	for i := 0; i < len(header.Values); i++ {
		domain_file := path.Join(output_dir, "domains", file, fmt.Sprintf("%d.values", i))
		if !exists(domain_file) {
			continue
		}

		values, err := readLines(domain_file, 100)
		if err == nil {
			fmt.Fprintf(fout, "%d %s\n", i, classifyValues(values))
		} else {
			panic(err)
		}
	}
}

func DoClassifyDomainsFromFiles(fanout int, files <-chan string) <-chan int {
	out := make(chan int)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			n := 0
			for file := range files {
				classifyDomains(file)
				n += 1
				if n%100 == 0 {
					out <- n
					n = 0
				}
			}
			out <- n
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func getTextDomains(file string) (indices []int) {
	typesFile := path.Join(output_dir, "domains", file, "types")
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

func normalize(w string) string {
	return strings.ToLower(w)
}

var patternSymb *regexp.Regexp

func init() {
	patternSymb = regexp.MustCompile(`[^a-z ]`)
}

func wordsFromLine(line string) []string {
	line = normalize(line)
	words := patternSymb.Split(line, -1)

	return words
}

func streamDomainWords(file string, index int, out chan *Domain) {
	filepath := path.Join(output_dir, "domains", file, fmt.Sprintf("%d.values", index))
	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	var values []string
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		words := wordsFromLine(scanner.Text())
		for _, word := range words {
			values = append(values, normalize(word))
			if len(values) >= 1000 {
				out <- &Domain{
					Filename: file,
					Index:    index,
					Values:   values,
				}
				values = nil
			}
		}
	}
}

func StreamDomainValuesFromFiles(fanout int, files <-chan string) <-chan *Domain {
	out := make(chan *Domain)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			for file := range files {
				for _, index := range getTextDomains(file) {
					streamDomainWords(file, index, out)
				}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func cleanEntityName(ent string) string {
	x := strings.Replace(strings.ToLower(ent), "_", " ", -1)
	i := strings.Index(x, "(")
	if i > 0 {
		x = x[0:i]
	}
	return strings.TrimSpace(x)
}

type Entities map[string]map[string]bool

func OpenYagoEntities() Entities {
	db, err := sql.Open("sqlite3", yago_db)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
        SELECT entity, category
        FROM types
    `)
	defer rows.Close()
	if err != nil {
		panic(err)
	}

	var m = make(map[string]map[string]bool)

	total := 16920001
	counter := 0
	s := GetNow()
	for rows.Next() {
		var ent string
		var cat string
		err = rows.Scan(&ent, &cat)
		if err != nil {
			panic(err)
		}

		ent = cleanEntityName(ent)

		if _, ok := m[ent]; !ok {
			m[ent] = make(map[string]bool)
		}
		m[ent][cat] = true
		counter += 1
		if counter%40000 == 0 {
			fmt.Printf("[entitydb] %d/%d rows in %.2f seconds\n", counter, total, GetNow()-s)
		}
	}

	return Entities(m)
}

func (ent Entities) Annotate(value string) map[string]bool {
	return ent[value]
}
