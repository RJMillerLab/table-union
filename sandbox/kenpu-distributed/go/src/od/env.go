package od

import "os"

const PARALLEL = 64
const MIN_DOMSIZE = 5

// Environment variables required to
// locate the necessary input files
var opendata_dir = os.Getenv("OPENDATA_DIR")
var opendata_list = os.Getenv("OPENDATA_LIST")

// Environment variable required to
// write output
var output_dir = os.Getenv("OUTPUT_DIR")

// Environment variable for the Yago database
var yago_db = os.Getenv("YAGO_DB")

func CheckEnv() {
	if opendata_dir == "" || output_dir == "" || yago_db == "" {
		panic("Environment missing")
	}
}
