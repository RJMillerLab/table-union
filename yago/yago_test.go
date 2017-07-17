package yago

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func Test_Yago_Init(t *testing.T) {
	yg := InitYago("/home/ekzhu/YAGO/yago.sqlite")

	_ = yg.Copy()
}
