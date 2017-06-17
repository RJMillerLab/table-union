package embedding

import (
	"database/sql"
	"fmt"

	fasttext "github.com/ekzhu/go-fasttext"
)

type InMemFastText struct {
	db       *sql.DB
	tokenFun func(string) []string
	transFun func(string) string
}

// Creates an in-memory FastText using an existing on-disk FastText Sqlite3 database.
func InitInMemoryFastText(dbFilename string, tokenFun func(string) []string, transFun func(string) string) (*InMemFastText, error) {
	db, err := sql.Open("sqlite3", ":memory:")
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
	return &InMemFastText{
		db:       db,
		tokenFun: tokenFun,
		transFun: transFun,
	}, nil
}

// Alaways close the FastText after finishing using it.
func (ft *InMemFastText) Close() error {
	return ft.db.Close()
}

// Get the embedding vector of a word
func (ft *InMemFastText) GetEmb(word string) ([]float64, error) {
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
func (ft *InMemFastText) GetValueEmb(value string) ([]float64, error) {
	tokens := tokenize(value, ft.tokenFun, ft.transFun)
	return ft.getTokenizedValueEmb(tokens)
}

// Returns the domain embedding by summation given the
// distinct values and their frequencies
func (ft *InMemFastText) GetDomainEmbSum(values []string, freqs []int) ([]float64, error) {
	var sum []float64
	for i, value := range values {
		freq := freqs[i]
		tokens := tokenize(value, ft.tokenFun, ft.transFun)
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

// Returns the embedding vector of a tokenized data value
func (ft *InMemFastText) getTokenizedValueEmb(tokenizedValue []string) ([]float64, error) {
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
func tokenize(v string, tokenFun func(string) []string, transFun func(string) string) []string {
	v = transFun(v)
	tokens := tokenFun(v)
	for i, t := range tokens {
		tokens[i] = transFun(t)
	}
	return tokens
}
