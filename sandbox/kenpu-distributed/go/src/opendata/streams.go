package opendata

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

func readLines(filename string, maxLines int) (lines []string, err error) {
	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if maxLines > 0 && maxLines <= len(lines) {
			break
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

func classifyValues(values []string) string {
	return "unknown"
}

func classifyDomains(file string) {
	header := GetDomainHeader(file)
	fout, err := os.OpenFile(path.Join(output_dir, "domains", file, "types"), os.O_CREATE|os.O_WRONLY, 0644)
	defer fout.Close()

	if err != nil {
		return
	}

	for i := 0; i < len(header.Values); i++ {
		domain_file := path.Join(output_dir, "domains", file, strconv.Itoa(i), "values")
		values, err := readLines(domain_file, 100)
		if err != nil {
			fmt.Fprintf(fout, "%d %s\n", i, classifyValues(values))
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
