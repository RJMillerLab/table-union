package server

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/RJMillerLab/fastTextHomeWork/search"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
	"github.com/gin-gonic/gin"
)

type Server struct {
	ft     *fasttext.FastText
	ts     *wikitable.WikiTableStore
	si     *search.SearchIndex
	router *gin.Engine
}

type QueryRequest struct {
	Table *datatable.DataTable `json:"query_table"`
	K     int                  `json:"k"`
}

type QueryResponse struct {
	Results [][]QueryResult `json:"query_results"`
}

type QueryResult struct {
	TableID     string `json:"table_id"`
	ColumnIndex int    `json:"column_index"`
}

func NewServer(ft *fasttext.FastText, ts *wikitable.WikiTableStore,
	si *search.SearchIndex) *Server {
	s := &Server{
		ft:     ft,
		ts:     ts,
		si:     si,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
	return s
}

func (s *Server) Run(port string) error {
	return s.router.Run(":" + port)
}

func (s *Server) Close() error {
	if err := s.ft.Close(); err != nil {
		return err
	}
	return s.si.Close()
}

func (s *Server) queryHandler(c *gin.Context) {
	body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1048576))
	if err != nil {
		log.Println(err)
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var queryRequest QueryRequest
	if err := json.Unmarshal(body, &queryRequest); err != nil {
		log.Println(err)
		c.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}
	// Create embeddings
	vecs := make([][]float64, queryRequest.Table.NumCol())
	for i := range vecs {
		vec, err := s.si.GetEmb(queryRequest.Table.GetColumn(i))
		if err != nil {
			log.Printf("Embedding not found for column %d", i)
			continue
		}
		vecs[i] = vec
	}
	// Query index
	results := make([][]QueryResult, len(vecs))
	for i := range vecs {
		result := make([]QueryResult, 0)
		if vecs[i] != nil {
			embs := s.si.TopK(vecs[i], queryRequest.K)
			for _, emb := range embs {
				result = append(result, QueryResult{
					TableID:     emb.TableID,
					ColumnIndex: emb.ColumnIndex,
				})
			}
		}
		results[i] = result
	}
	response := QueryResponse{
		Results: results,
	}
	c.JSON(http.StatusOK, response)
}
