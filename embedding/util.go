package embedding

import (
	"encoding/binary"
	"io"
	"math"
	"os"
)

func WriteVec(vec []float64, order binary.ByteOrder, file io.Writer) error {
	for _, v := range vec {
		err := binary.Write(file, order, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteVecToDisk(vec []float64, order binary.ByteOrder, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	binVec := VecToBytes(vec, order)
	if _, err := file.Write(binVec); err != nil {
		return err
	}
	return nil
}

func ReadVecFromDisk(filename string, order binary.ByteOrder) ([]float64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stats, serr := file.Stat()
	if serr != nil {
		return nil, serr
	}

	var size int64 = stats.Size()
	binVec := make([]byte, size)
	if _, rerr := file.Read(binVec); rerr != nil {
		return nil, rerr
	}
	vec, verr := BytesToVec(binVec, order)
	return vec, verr
}

func Cosine(x, y []float64) float64 {
	if len(x) != len(y) {
		panic("Length of vectors not equal")
	}
	dot := 0.0
	modX, modY := 0.0, 0.0
	for i := range x {
		dot += x[i] * y[i]
		modX += x[i] * x[i]
		modY += y[i] * y[i]
	}
	return dot / (math.Sqrt(modX) * math.Sqrt(modY))
}
