package opendata

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

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

func (domain *Domain) Save() int {
	filepath := path.Join(output_dir, "domains", domain.Filename, strconv.Itoa(domain.Index), "values")
	// Create the file if necessary
	if _, err := os.Stat(filepath); err != nil {
		dir, _ := path.Split(filepath)
		os.MkdirAll(dir, 0755)
	}
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		defer f.Close()
		for _, value := range domain.Values {
			fmt.Fprintln(f, value)
		}
	}
	return len(domain.Values)
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
				domains[c].Values = append(domains[c].Values, row[c])
			}
		}
	}
	return domains
}

func makeDomains(filenames <-chan string, out chan *Domain) {
	for filename := range filenames {
		f, err := os.Open(Filepath(filename))
		if err != nil {
			continue
		}
		defer f.Close()

		rdr := csv.NewReader(f)
		// Skip the first row
		row, err := rdr.Read()
		if err != nil {
			continue
		}
		width := len(row)

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
				if len(cells) == 100 {
					for _, domain := range domainsFromCells(cells, filename, width) {
						out <- domain
					}
					cells = nil
				}
			}
		}
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
			for domain := range queue {
				n := domain.Save()
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
