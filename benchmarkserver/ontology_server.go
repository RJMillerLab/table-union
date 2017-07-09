package benchmarkserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type OntologyJaccardServer struct {
	ui     *JaccardUnionIndex
	oi     *JaccardUnionIndex
	router *gin.Engine
}

type OntologyJaccardQueryRequest struct {
	Vecs             [][]uint64 `json:"table"`
	OntVecs          [][]uint64 `json:"onttable"`
	K                int        `json:"k"`
	N                int        `json:"n"`
	OntCardinality   []int      `json:"ontcard"`
	NoOntCardinality []int      `json:"noontcard"`
}

func NewOntologyJaccardServer(ui *JaccardUnionIndex, oi *JaccardUnionIndex) *OntologyJaccardServer {
	s := &OntologyJaccardServer{
		ui:     ui,
		oi:     oi,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
	log.Printf("New ontlogy jaccard server for experiments.")
	return s
}

func (s *OntologyJaccardServer) Run(port string) error {
	return s.router.Run(":" + port)
}

func (s *OntologyJaccardServer) Close() error {
	return nil
}

func (s *OntologyJaccardServer) queryHandler(c *gin.Context) {
	body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1048576))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var queryRequest OntologyJaccardQueryRequest
	if err := json.Unmarshal(body, &queryRequest); err != nil {
		c.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}
	// Query index
	searchResults := make([]QueryResult, 0)
	//start := time.Now()
	queryResults := s.OntQueryOrderAll(queryRequest.Vecs, queryRequest.OntVecs, queryRequest.N, queryRequest.K, queryRequest.NoOntCardinality, queryRequest.OntCardinality)
	//dur := time.Since(start)
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
