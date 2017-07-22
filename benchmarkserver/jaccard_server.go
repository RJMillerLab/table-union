package benchmarkserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type JaccardServer struct {
	ui     *JaccardUnionIndex
	router *gin.Engine
}

type JaccardQueryRequest struct {
	Vecs        [][]uint64 `json:"table"`
	K           int        `json:"k"`
	N           int        `json:"n"`
	Cardinality []int      `json:"cardinality"`
}

func NewJaccardServer(ui *JaccardUnionIndex) *JaccardServer {
	s := &JaccardServer{
		ui:     ui,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
	log.Printf("New jaccard server for experiments.")
	return s
}

func (s *JaccardServer) Run(port string) error {
	return s.router.Run(":" + port)
}

func (s *JaccardServer) Close() error {
	return nil
}

func (s *JaccardServer) queryHandler(c *gin.Context) {
	body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1048576))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var queryRequest JaccardQueryRequest
	if err := json.Unmarshal(body, &queryRequest); err != nil {
		c.AbortWithStatus(http.StatusUnprocessableEntity)
		log.Printf("abort query.")
		return
	}
	// Query index
	searchResults := make([]QueryResult, 0)
	queryResults := s.ui.QueryOrderAll(queryRequest.Vecs, queryRequest.N, queryRequest.K, queryRequest.Cardinality)
	for result := range queryResults {
		union := Union{
			CandTableID:    result.CandidateTableID,
			CandHeader:     getHeaders(result.CandidateTableID, s.ui.domainDir),
			CandTextHeader: getTextHeaders(result.CandidateTableID, s.ui.domainDir),
			Alignment:      result.Alignment,
			Kunioability:   result.Alignment[len(result.Alignment)-1].Sim,
			K:              result.K,
			N:              result.N,
			Duration:       result.Duration,
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
