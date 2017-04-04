package embserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/ekzhu/counter"
	fasttext "github.com/ekzhu/go-fasttext"
)

// Get the embedding vector of a column by taking the average of the distinct values (tokenized) vectors.
func getColEmb(ft *fasttext.FastText, transFun func(string) string, column []string) ([]float64, error) {
	values := mkTokenizedValues(column, transFun)
	var vec []float64
	var count int
	for tokens := range values {
		valueVec, err := getValueEmb(ft, tokens)
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

// Get the embedding vector of a tokenized value by sum all the tokens' vectors
func getValueEmb(ft *fasttext.FastText, tokenizedValue []string) ([]float64, error) {
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
	//	for i, v := range valueVec {
	//		valueVec[i] = v / float64(count)
	//	}
	return valueVec, nil
}

// Produce a channel of distinct values (tokenized)
func mkTokenizedValues(values []string, transFun func(string) string) chan []string {
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
			tokens := strings.Split(v, " ")
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

func dotProduct(x, y []float64) float64 {
	if len(x) != len(y) {
		panic("Length of vectors not equal")
	}
	p := 0.0
	for i := range x {
		p += x[i] * y[i]
	}
	return p
}

func add(dst, src []float64) {
	if len(dst) != len(src) {
		panic("Length of vectors not equal")
	}
	for i := range src {
		dst[i] = dst[i] + src[i]
	}
}

func vecToBytes(vec []float64, order binary.ByteOrder) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		binary.Write(buf, order, v)
	}
	return buf.Bytes()
}

func bytesToVec(data []byte, order binary.ByteOrder) ([]float64, error) {
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

func fromColumnID(columnID string) (tableID string, columnIndex int) {
	items := strings.Split(columnID, ":")
	if len(items) != 2 {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	tableID = items[0]
	var err error
	columnIndex, err = strconv.Atoi(items[1])
	if err != nil {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	return
}

func toColumnID(tableID string, columnIndex int) (columnID string) {
	columnID = fmt.Sprintf("%s:%d", tableID, columnIndex)
	return
}
