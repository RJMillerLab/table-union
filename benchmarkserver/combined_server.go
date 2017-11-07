package benchmarkserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/gin-gonic/gin"
)

type CombinedServer struct {
	// U_set index
	seti *JaccardUnionIndex
	// U_semset and U_sem indexes
	semi    *JaccardUnionIndex
	semseti *JaccardUnionIndex
	// U_nl index
	nli       *UnionIndex
	router    *gin.Engine
	tableCDF  map[int]opendata.CDF
	semCDF    opendata.CDF
	setCDF    opendata.CDF
	semsetCDF opendata.CDF
	nlCDF     opendata.CDF
}

type CombinedQueryRequest struct {
	SetVecs      [][]uint64  `json:"settable"`
	OntVecs      [][]uint64  `json:"onttable"`
	NoOntVecs    [][]uint64  `json:"noonttable"`
	NlMeans      [][]float64 `json:"nlmean"`
	NlCovars     [][]float64 `json:"nlcovariance"`
	N            int         `json:"n"`
	SetCards     []int       `json:"setcard"`
	OntCards     []int       `json:"ontcard"`
	NoOntCards   []int       `json:"noontcard"`
	NlCards      []int       `json:"nlcard"`
	QueryTableID string      `json:"querytableid"`
}

func NewCombinedServer(seti, semi, semseti *JaccardUnionIndex, nli *UnionIndex) *CombinedServer {
	setCDF, semCDF, semsetCDF, nlCDF, tableCDF := opendata.LoadCDF()
	s := &CombinedServer{
		seti:      seti,
		semi:      semi,
		semseti:   semseti,
		nli:       nli,
		semCDF:    semCDF,
		setCDF:    setCDF,
		semsetCDF: semsetCDF,
		nlCDF:     nlCDF,
		tableCDF:  tableCDF,
		router:    gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
	log.Printf("New combined server for experiments.")
	return s
}

func (s *CombinedServer) Run(port string) error {
	return s.router.Run(":" + port)
}

func (s *CombinedServer) Close() error {
	return nil
}

func (s *CombinedServer) queryHandler(c *gin.Context) {
	body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1048576))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	var queryRequest CombinedQueryRequest
	if err := json.Unmarshal(body, &queryRequest); err != nil {
		c.AbortWithStatus(http.StatusUnprocessableEntity)
		return
	}
	// Query index
	searchResults := make([]QueryResult, 0)
	queryResults := s.CombinedOrderAll(queryRequest.NlMeans, queryRequest.NlCovars, queryRequest.SetVecs, queryRequest.NoOntVecs, queryRequest.OntVecs, queryRequest.N, queryRequest.NoOntCards, queryRequest.OntCards, queryRequest.NlCards, queryRequest.SetCards, queryRequest.QueryTableID)
	for result := range queryResults {
		union := Union{
			CandTableID:              result.CandidateTableID,
			CandHeader:               getHeaders(result.CandidateTableID, s.seti.domainDir),
			CandTextHeader:           getTextHeaders(result.CandidateTableID, s.seti.domainDir),
			Alignment:                result.Alignment,
			N:                        result.N,
			Duration:                 result.Duration,
			CUnionabilityScores:      result.CUnionabilityScores,
			CUnionabilityPercentiles: result.CUnionabilityPercentiles,
			BestC: result.BestC,
		}
		log.Printf("alignment: %d", len(result.Alignment))

		searchResults = append(searchResults, QueryResult{
			TableUnion: union,
		})
	}
	response := QueryResponse{
		Result: searchResults,
	}
	c.JSON(http.StatusOK, response)
}
