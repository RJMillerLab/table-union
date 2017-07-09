package embedding

import (
	"database/sql"
	"fmt"
	"log"

	fasttext "github.com/ekzhu/go-fasttext"
	"github.com/gonum/matrix/mat64"
	"github.com/gonum/stat"
)

type FastText struct {
	db       *sql.DB
	tokenFun func(string) []string
	transFun func(string) string
}

// Creates an in-memory FastText using an existing on-disk FastText Sqlite3 database.
func InitFastText(dbFilename string, tokenFun func(string) []string, transFun func(string) string) (*FastText, error) {
	db, err := sql.Open("sqlite3", dbFilename+"?cache=shared")
	if err != nil {
		return nil, err
	}
	return &FastText{
		db:       db,
		tokenFun: tokenFun,
		transFun: transFun,
	}, nil
}

// Creates an in-memory FastText using an existing on-disk FastText Sqlite3 database.
func InitInMemoryFastText(dbFilename string, tokenFun func(string) []string, transFun func(string) string) (*FastText, error) {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	_, err = db.Exec(fmt.Sprintf(`
	attach database '%s' as disk;
	`, dbFilename))
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`create table fasttext as select * from disk.fasttext;`)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`create index inx_ft on fasttext(word);`)
	if err != nil {
		return nil, err
	}
	return &FastText{
		db:       db,
		tokenFun: tokenFun,
		transFun: transFun,
	}, nil
}

// Alaways close the FastText after finishing using it.
func (ft *FastText) Close() error {
	return ft.db.Close()
}

// Get all words that exist in the database
func (ft *FastText) GetAllWords() ([]string, error) {
	var count int
	if err := ft.db.QueryRow(`SELECT count(word) FROM fasttext;`).Scan(&count); err != nil {
		return nil, err
	}
	words := make([]string, 0, count)
	rows, err := ft.db.Query(`SELECT word FROM fasttext;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var word string
		if err := rows.Scan(&word); err != nil {
			return words, err
		}
		words = append(words, word)
	}
	return words, rows.Err()
}

// Get the embedding vector of a word
func (ft *FastText) GetEmb(word string) ([]float64, error) {
	var binVec []byte
	err := ft.db.QueryRow(`SELECT emb FROM fasttext WHERE word=?;`, word).Scan(&binVec)
	if err == sql.ErrNoRows {
		return nil, ErrNoEmbFound
	}
	if err != nil {
		panic(err)
	}
	vec, err := BytesToVec(binVec, fasttext.ByteOrder)
	return vec, err
}

// Get the embedding vector of a data value, which is the sum of word embeddings
func (ft *FastText) GetValueEmb(value string) ([]float64, error) {
	tokens := Tokenize(value, ft.tokenFun, ft.transFun)
	return ft.getTokenizedValueEmb(tokens)
}

// Returns the domain embedding by summation given the
// distinct values and their frequencies
func (ft *FastText) GetDomainEmbSum(values []string, freqs []int) ([]float64, error) {
	var sum []float64
	for i, value := range values {
		freq := freqs[i]
		tokens := Tokenize(value, ft.tokenFun, ft.transFun)
		vec, err := ft.getTokenizedValueEmb(tokens)
		if err != nil {
			continue
		}
		for j, x := range vec {
			vec[j] = x * float64(freq)
		}
		if sum == nil {
			sum = vec
		} else {
			add(sum, vec)
		}
	}
	if sum == nil {
		return nil, ErrNoEmbFound
	}
	return sum, nil
}

// Returns the mean of domain embedding matrix
func (ft *FastText) GetDomainEmbMean(values []string, freqs []int) ([]float64, error) {
	var sum []float64
	ftValuesNum := 0
	for i, value := range values {
		freq := freqs[i]
		tokens := Tokenize(value, ft.tokenFun, ft.transFun)
		vec, err := ft.getTokenizedValueEmb(tokens)
		if err != nil {
			continue
		}
		ftValuesNum += freq
		for j, x := range vec {
			vec[j] = x * float64(freq)
		}
		if sum == nil {
			sum = vec
		} else {
			add(sum, vec)
		}
	}
	if sum == nil {
		return nil, ErrNoEmbFound
	}
	mean := multVector(sum, 1.0/float64(ftValuesNum))
	return mean, nil
}

// Returns the covariance matrix of the domain
func (ft *FastText) GetDomainCovariance(values []string, freqs []int) []float64 {
	log.Printf("starting computing covar.")
	embs := make([]float64, 0)
	ftValuesNum := 0
	for i, value := range values {
		tokens := Tokenize(value, ft.tokenFun, ft.transFun)
		vec, err := ft.getTokenizedValueEmb(tokens)
		if err != nil {
			continue
		}
		ftValuesNum += freqs[i]
		for f := 0; f < freqs[i]; f += 1 {
			embs = append(embs, vec...)
		}
	}
	log.Printf("len(embs): %d", len(embs))
	// computing covariance
	matrix := mat64.NewDense(ftValuesNum, 300, embs)
	cov := stat.CovarianceMatrix(nil, matrix, nil)
	log.Printf("done computing covar.")
	return flattenMatrix(cov)
}

// Returns the mean of domain embedding matrix
func (ft *FastText) GetDomainEmbMeanCovar(values []string, freqs []int) ([]float64, []float64, error) {
	var embs [][]float64
	var sum []float64
	ftValuesNum := 0
	for i, value := range values {
		freq := freqs[i]
		tokens := Tokenize(value, ft.tokenFun, ft.transFun)
		vec, err := ft.getTokenizedValueEmb(tokens)
		if err != nil {
			continue
		}
		ftValuesNum += freq
		for j, x := range vec {
			vec[j] = x * float64(freq)
		}
		embs = append(embs, vec)
		if sum == nil {
			sum = vec
		} else {
			add(sum, vec)
		}
	}
	if sum == nil {
		return nil, nil, ErrNoEmbFound
	}
	mean := multVector(sum, 1.0/float64(ftValuesNum))
	// calculating covar
	covar := make([]float64, len(mean))
	covarSum := make([]float64, len(mean))
	for _, emb := range embs {
		for i, e := range emb {
			covarSum[i] += ((e - mean[i]) * (e - mean[i]))
		}
	}
	for i, _ := range mean {
		covar[i] = covarSum[i] / float64(len(embs)-1)
	}
	return mean, covar, nil
}

func multVector(v []float64, s float64) []float64 {
	sv := make([]float64, len(v))
	for i := 0; i < len(v); i++ {
		sv[i] = v[i] / s
	}
	return sv
}

func flattenMatrix(a mat64.Matrix) []float64 {
	r, _ := a.Dims()
	f := make([]float64, 0)
	for i := 0; i < r; i++ {
		f = append(f, mat64.Row(nil, i, a)...)
	}
	return f
}

// Returns the embedding vector of a tokenized data value
func (ft *FastText) getTokenizedValueEmb(tokenizedValue []string) ([]float64, error) {
	var valueVec []float64
	var count int
	for _, token := range tokenizedValue {
		emb, err := ft.GetEmb(token)
		if err == ErrNoEmbFound {
			continue
		}
		if err != nil {
			panic(err)
		}
		if valueVec == nil {
			valueVec = emb
		} else {
			add(valueVec, emb)
		}
		count++
	}
	if valueVec == nil {
		return nil, ErrNoEmbFound
	}
	return valueVec, nil
}

// Tokenize the value v with tokenization and transformation function
func Tokenize(v string, tokenFun func(string) []string, transFun func(string) string) []string {
	v = transFun(v)
	tokens := tokenFun(v)
	for i, t := range tokens {
		tokens[i] = transFun(t)
	}
	return tokens
}
