package od

import (
	"bufio"
	"os"
	"sync"
)

// This enumerates all the datafiles frome an existing
// opendata.list which contains one line for each CSV
// file.  It's built using python/build-opendata-index.py
func GetDatafileChan() <-chan *Datafile {
	datafiles := make(chan *Datafile)
	go func() {
		f, err := os.Open(opendata_list)
		if err == nil {
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				datafile := ParseDatafile(scanner.Text())
				datafiles <- &datafile
			}
		} else {
			panic("Cannot load " + opendata_list)
		}
		close(datafiles)
	}()

	return datafiles
}

func GetDomains(datafiles <-chan *Datafile, nWorkers int) <-chan *Domain {
	wg := &sync.WaitGroup{}
	wg.Add(nWorkers)

	domChan := make(chan *Domain)

	for workerIndex := 0; workerIndex < nWorkers; workerIndex++ {
		go func(id int) {
			for df := range datafiles {
				domains := df.GetDomains()
				for _, dom := range domains {
					if dom.IsGood() {
						domChan <- dom
					}
				}
			}
			wg.Done()
		}(workerIndex)
	}
	go func() {
		wg.Wait()
		close(domChan)
	}()

	return domChan
}

type ProgressCounter struct {
	Domains int
	Values  int
}

func SaveDomainValues(domains <-chan *Domain, nWorkers int, progress chan<- ProgressCounter) <-chan bool {
	wg := &sync.WaitGroup{}
	done := make(chan bool)

	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go func(id int) {
			n := ProgressCounter{}

			for dom := range domains {
				dom.SaveValues()
				n.Domains += 1
				n.Values += len(dom.Values)

				if n.Domains%2 == 0 {
					progress <- n
					n.Domains = 0
					n.Values = 0
				}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		done <- true
		close(done)
	}()

	return done
}
