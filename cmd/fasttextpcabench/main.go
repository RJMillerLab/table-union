package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/RJMillerLab/table-union/embedding"
	fasttext "github.com/ekzhu/go-fasttext"
	"github.com/gonum/matrix/mat64"
	"github.com/gonum/stat"
	_ "github.com/mattn/go-sqlite3"
)

var (
	scales = []int{10, 100, 1000, 10000, 100000, 1000000}
)

func main() {
	var fastTextSqliteDB string
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.Parse()
	db, err := sql.Open("sqlite3", fastTextSqliteDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for _, scale := range scales {
		matrix := getEmbs(db, scale)
		dur := pca(matrix)
		fmt.Printf("n = %d:\tPCA took %f\n", scale, dur.Seconds())
	}
}

func pca(matrix *mat64.Dense) time.Duration {
	start := time.Now()
	var pc stat.PC
	if okay := pc.PrincipalComponents(matrix, nil); !okay {
		panic("PCA failed")
	}
	return time.Now().Sub(start)
}

func getEmbs(db *sql.DB, n int) *mat64.Dense {
	data := make([]float64, 0)
	rows, err := db.Query("SELECT emb FROM fasttext LIMIT ?", n)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var binVec []byte
		if err := rows.Scan(&binVec); err != nil {
			panic(err)
		}
		vec, err := embedding.BytesToVec(binVec, fasttext.ByteOrder)
		if err != nil {
			panic(err)
		}
		data = append(data, vec...)
	}
	matrix := mat64.NewDense(n, fasttext.Dim, data)
	return matrix
}
