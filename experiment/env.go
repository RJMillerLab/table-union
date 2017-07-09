package experiment

import (
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const PARALLEL = 64
const MIN_DOMSIZE = 5

// Environment variables required to
// locate the necessary input files
var OpendataDir = os.Getenv("OPENDATA_DIR")

var ExperimentDB = os.Getenv("EXPERIMENT_DB")
var ExperimentDB2 = os.Getenv("EXPERIMENT_DB2")
var ExperimentTable = os.Getenv("EXPERIMENT_TABLE")
var ExperimentTable2 = os.Getenv("EXPERIMENT_TABLE2")
var ExpansionDB = os.Getenv("EXPANSION_DB")
var ExpansionTable = os.Getenv("EXPANSION_TABLE")
var ExpansionDB2 = os.Getenv("EXPANSION_DB2")
var ExpansionTable2 = os.Getenv("EXPANSION_TABLE2")
var SharedDB = os.Getenv("SHARED_DB")
var SharedTable = os.Getenv("SHARED_TABLE")

func CheckEnv() {
	if ExperimentDB == "" || ExperimentTable == "" || ExpansionDB == "" || ExpansionTable == "" {
		panic("Environment missing")
	}
}

func GetNow() float64 {
	return float64(time.Now().UnixNano()) / 1E9
}
