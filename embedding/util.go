package embedding

import (
	"encoding/binary"
	"io"
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