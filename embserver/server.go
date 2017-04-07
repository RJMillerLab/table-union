package embserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/RJMillerLab/table-union/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
	"github.com/gin-gonic/gin"
)

type Server struct {
	ft     *fasttext.FastText
	ts     *wikitable.WikiTableStore
	si     *SearchIndex
	router *gin.Engine
}

type QueryRequest struct {
	Vec []float64 `json:"vec"`
	K   int       `json:"k"`
}

type QueryResponse struct {
	Result []QueryResult `json:"result"`
}

type QueryResult struct {
	TableID     string    `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Vec         []float64 `json:"vec"`
}

func NewServer(ft *fasttext.FastText, ts *wikitable.WikiTableStore,
	si *SearchIndex) *Server {
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
	// Query index
	result := make([]QueryResult, 0)
	embs := s.si.TopK(queryRequest.Vec, queryRequest.K)
	for _, emb := range embs {
		result = append(result, QueryResult{
			TableID:     emb.TableID,
			ColumnIndex: emb.ColumnIndex,
			Vec:         emb.Vec,
		})
	}
	response := QueryResponse{
		Result: result,
	}
	c.JSON(http.StatusOK, response)
}
