package embedding

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ekzhu/counter"
	fasttext "github.com/ekzhu/go-fasttext"
	"github.com/gonum/matrix/mat64"
	"github.com/gonum/stat"
)

var (
	ErrNoEmbFound = errors.New("No embedding found")
	ErrPCAFailure = errors.New("PCA Failed")
)

// Get the embedding vector of a column by taking the average of the distinct values (tokenized) vectors.
func GetDomainEmbAve(ft *fasttext.FastText, tokenFun func(string) []string, transFun func(string) string, column []string) ([]float64, error) {
	values := TokenizedValues(column, tokenFun, transFun)
	var vec []float64
	var count int
	for tokens := range values {
		valueVec, err := GetValueEmb(ft, tokens)
		if err != nil {
			continue
		}
		if vec == nil {
			vec = valueVec
		} else {
			add(vec, valueVec)
		}
		count++
	}
	if vec == nil {
		return nil, ErrNoEmbFound
	}
	for i, v := range vec {
		vec[i] = v / float64(count)
	}
	return vec, nil
}

// Get the embedding vector of a column domain by taking the first principal
// component of the distinct values (tokenized) vectors
func GetDomainEmbPCA(ft *fasttext.FastText, tokenFun func(string) []string, transFun func(string) string, column []string) ([]float64, error) {
	values := TokenizedValues(column, tokenFun, transFun)
	var data []float64
	var count int
	for tokens := range values {
		valueVec, err := GetValueEmb(ft, tokens)
		if err != nil {
			continue
		}
		if data == nil {
			data = valueVec
		} else {
			data = append(data, valueVec...)
		}
		count++
	}
	if count == 0 {
		return nil, ErrNoEmbFound
	}
	matrix := mat64.NewDense(count, fasttext.Dim, data)
	var pc stat.PC
	if ok := pc.PrincipalComponents(matrix, nil); !ok {
		return nil, ErrPCAFailure
	}
	pcs := pc.Vectors(nil)
	vec := make([]float64, fasttext.Dim)
	mat64.Col(vec, 0, pcs)
	return vec, nil
}

// Get the embedding vector of a tokenized value by sum all the tokens' vectors
func GetValueEmb(ft *fasttext.FastText, tokenizedValue []string) ([]float64, error) {
	var valueVec []float64
	var count int
	for _, token := range tokenizedValue {
		emb, err := ft.GetEmb(token)
		if err == fasttext.ErrNoEmbFound {
			continue
		}
		if err != nil {
			panic(err)
		}
		if valueVec == nil {
			valueVec = emb.Vec
		} else {
			add(valueVec, emb.Vec)
		}
		count++
	}
	if valueVec == nil {
		return nil, ErrNoEmbFound
	}
	return valueVec, nil
}

// Produce a channel of distinct values (tokenized)
func TokenizedValues(values []string, tokenFun func(string) []string, transFun func(string) string) chan []string {
	out := make(chan []string)
	go func() {
		counter := counter.NewCounter()
		for _, v := range values {
			v = transFun(v)
			if counter.Has(v) {
				continue
			}
			counter.Update(v)
			// Tokenize
			tokens := tokenFun(v)
			if len(tokens) > 5 {
				// Skip text values
				continue
			}
			for i, t := range tokens {
				tokens[i] = transFun(t)
			}
			out <- tokens
		}
		close(out)
	}()
	return out
}

func add(dst, src []float64) {
	if len(dst) != len(src) {
		panic("Length of vectors not equal")
	}
	for i := range src {
		dst[i] = dst[i] + src[i]
	}
}

func VecToBytes(vec []float64, order binary.ByteOrder) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		binary.Write(buf, order, v)
	}
	return buf.Bytes()
}

func BytesToVec(data []byte, order binary.ByteOrder) ([]float64, error) {
	size := len(data) / 8
	vec := make([]float64, size)
	buf := bytes.NewReader(data)
	var v float64
	for i := range vec {
		if err := binary.Read(buf, order, &v); err != nil {
			return nil, err
		}
		vec[i] = v
	}
	return vec, nil
}
