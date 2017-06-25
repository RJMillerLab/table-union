package benchmarkserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	ui     *UnionIndex
	jui    *JaccardUnionIndex
	router *gin.Engine
}

type QueryRequest struct {
	Vecs [][]float64 `json:"table"`
	K    int         `json:"k"`
	N    int         `json:"n"`
}

type QueryResponse struct {
	Result []QueryResult `json:"result"`
}

type QueryResult struct {
	TableUnion Union `json:"union"`
}

type Union struct {
	QueryTableID    string
	CandTableID     string
	CandHeader      []string
	Alignment       []Pair // query to candidate table
	QueryTextHeader []string
	QueryHeader     []string
	Kunioability    float64
	K               int
	N               int
	Duration        float64
}

func NewServer(ui *UnionIndex) *Server {
	s := &Server{
		ui:     ui,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
	log.Printf("New emb server for experiments.")
	return s
}

func (s *Server) Run(port string) error {
	return s.router.Run(":" + port)
}

func (s *Server) Close() error {
	return nil
}

func (s *Server) queryHandler(c *gin.Context) {
	body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1048576))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var queryRequest QueryRequest
	if err := json.Unmarshal(body, &queryRequest); err != nil {
		c.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}
	// Query index
	searchResults := make([]QueryResult, 0)
	//start := time.Now()
	queryResults := s.ui.QueryOrderAll(queryRequest.Vecs, queryRequest.N, queryRequest.K)
	//dur := time.Since(start)
	for result := range queryResults {
		union := Union{
			CandTableID:  result.CandidateTableID,
			CandHeader:   getHeaders(result.CandidateTableID, s.ui.domainDir),
			Alignment:    result.Alignment,
			Kunioability: result.Alignment[len(result.Alignment)-1].Sim,
			K:            result.K,
			N:            result.N,
			Duration:     result.Duration,
		}

		searchResults = append(searchResults, QueryResult{
			TableUnion: union,
		})
	}
	response := QueryResponse{
		Result: searchResults,
	}
	c.JSON(http.StatusOK, response)
}
