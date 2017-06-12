package opendata

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"

	minhashlsh "github.com/ekzhu/minhash-lsh"
)

var (
	seed    = 1
	numHash = 256
)

type DomainSketch struct {
	Filename string              // the logical filename of the CSV file
	Index    int                 // the position of the domain in the csv file
	Sketch   *minhashlsh.Minhash // the minhash sketch
}

func DoMinhashDomainsFromFiles(fanout int, files <-chan string) <-chan *DomainSketch {
	out := make(chan *DomainSketch)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			for file := range files {
				for _, index := range getTextDomains(file) {
					minhashDomainWords(file, index, out)
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

func minhashDomainWords(file string, index int, out chan *DomainSketch) {
	filepath := path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.values", index))
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	mh := minhashlsh.NewMinhash(seed, numHash)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		words := wordsFromLine(scanner.Text())
		for _, word := range words {
			mh.Push([]byte(word))
		}
	}
	out <- &DomainSketch{
		Filename: file,
		Index:    index,
		Sketch:   mh,
	}
}

// Saves the domain skecthes from an input channel to disk
// Returns a channel of progress counter
func DoSaveDomainSketches(fanout int, sketches <-chan *DomainSketch) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int, sketches <-chan *DomainSketch) {
			for domain := range sketches {
				minhashFilename := domain.PhysicalFilename("minhash")
				err := writeMinhashSignature(domain.Sketch, minhashFilename)
				if err == nil {
					progress <- ProgressCounter{1}
				}
			}
			wg.Done()
		}(i, sketches)
	}
	go func() {
		wg.Wait()
		close(progress)
	}()
	return progress
}

func (domain *DomainSketch) PhysicalFilename(ext string) string {
	fullpath := path.Join(OutputDir, "domains", domain.Filename)

	if ext != "" {
		fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", domain.Index, ext))
	}

	return fullpath
}

func writeMinhashSignature(mh *minhashlsh.Minhash, filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for i := range mh.Signature() {
		if err := binary.Write(f, binary.BigEndian, mh.Signature()[i]); err != nil {
			return err
		}
	}
	return nil
}

func readMinhashSignature(filename string, numHash int) ([]uint64, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	signature := make([]uint64, numHash)
	for i := range signature {
		if err := binary.Read(f, binary.BigEndian, &(signature[i])); err != nil {
			return nil, err
		}
	}
	return signature, nil
}
