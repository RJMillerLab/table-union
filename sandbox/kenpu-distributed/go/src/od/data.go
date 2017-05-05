package od

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
)

// This is a helper function that returns seconds
// as float - just like python time.time()
func GetNow() float64 {
	return float64(time.Now().UnixNano()) / 1E9
}

// Helper function to determine if a value is good or not.
// TODO: use a better regexp later
func IsGoodValue(v string) bool {
	return strings.IndexAny(v, "abcdefghijklmnopqrstuvwxyz") >= 0
}

// Structure to represent the data files
// Each datafile is a CSV
type Datafile struct {
	Repo    string // Which repository
	Dataset string // Which dataset
	File    string // Most dataset has one file, but some have multiple
}

// Makes a Datafile instance based on a line in the open data
// list
func ParseDatafile(line string) Datafile {
	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 3)
	return Datafile{parts[0], parts[1], parts[2]}
}

func (f *Datafile) String() string {
	return f.GetFilename()
}

// Get the actual physical file path for the datafile (csv file)
func (f *Datafile) GetFilename() string {
	return path.Join(opendata_dir, f.Repo, f.Dataset, f.File)
}

// Most data sets have a metadata.json
// This gets the physical file path for the metadata.json file.
func (f *Datafile) GetMetadataFile() string {
	return path.Join(opendata_dir, f.Repo, "metadata.json")
}

// Checks if the CSV file actually exists
func (f *Datafile) Exists() bool {
	_, err := os.Stat(f.GetFilename())
	return err == nil
}

// Uses the golang CSV parser to get all the cells
// of the CSV file as a 2D array of strings.
// The first line is the headers (most of the times)
func (f *Datafile) GetCells() [][]string {
	file, err := os.Open(f.GetFilename())
	if err == nil {
		defer file.Close()
		rdr := csv.NewReader(file)
		records, err := rdr.ReadAll()
		if err == nil {
			return records
		}
	}
	return nil
}

// Get the domains from the datafile
// The first row is ignored (as we assume that it's the header)
// We also use the narrowest row as the column count
func (f *Datafile) GetDomains() []*Domain {
	if !f.Exists() {
		return nil
	}

	var colCount = 0
	var cells = f.GetCells()
	for _, row := range cells {
		rowWidth := len(row)
		if rowWidth < colCount || colCount == 0 {
			colCount = rowWidth
		}
	}

	domains := make([]*Domain, colCount)
	for col := 0; col < colCount; col++ {
		dom := &Domain{}
		dom.Datafile = f
		dom.Index = col
		for i, row := range cells {
			if i > 0 {
				value := strings.ToLower(row[col])
				if IsGoodValue(value) {
					dom.Values = append(dom.Values, value)
				}
			}
		}
		domains[col] = dom
	}

	return domains
}

// Structure to represent a domain
type Domain struct {
	Datafile *Datafile // the source datafile
	Index    int       // the position of the attribute for this domain
	Values   []string  // the values of the domain
}

func (d *Domain) String() string {
	return fmt.Sprintf("%s %s %s (%d) with %d values", d.Datafile.Repo, d.Datafile.Dataset, d.Datafile.File, d.Index, len(d.Values))
}

func (d *Domain) Size() int {
	return len(d.Values)
}

// A domain needs to be categorical
// and with minimal size
func (d *Domain) IsGood() bool {
	return d.Size() >= MIN_DOMSIZE
}

// Get the file to save the domain data to
func (d *Domain) SaveValues() {
	dirname := path.Join(output_dir, "domains", d.Datafile.Repo, d.Datafile.Dataset, d.Datafile.File, fmt.Sprintf("%d", d.Index))
	if _, err := os.Stat(dirname); err != nil {
		err = os.MkdirAll(dirname, 0744)
		if err != nil {
			panic(err)
		}
	}
	f, err := os.OpenFile(path.Join(dirname, "values"), os.O_WRONLY|os.O_CREATE, 0744)
	if err == nil {
		for _, value := range d.Values {
			fmt.Fprintln(f, value)
		}
		f.Close()
	} else {
		panic(err)
	}
}
