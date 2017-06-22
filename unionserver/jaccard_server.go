package unionserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type JaccardServer struct {
	ui     *JaccardUnionIndex
	router *gin.Engine
}

type JaccardQueryRequest struct {
	Vecs [][]uint64 `json:"table"`
	K    int        `json:"k"`
	N    int        `json:"n"`
}

func NewJaccardServer(ui *JaccardUnionIndex) *JaccardServer {
	s := &JaccardServer{
		ui:     ui,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
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
		return
	}
	// Query index
	result := make([]QueryResult, 0)
	start := time.Now()
	queryResults := s.ui.QueryOrderAll(queryRequest.Vecs, queryRequest.N, queryRequest.K)
	dur := time.Since(start)
	for unionableTablePairs := range queryResults {
		union := Union{
			CandTableID:  unionableTablePairs[0].CandTableID,
			CandHeader:   getHeaders(unionableTablePairs[0].CandTableID, s.ui.domainDir),
			Alignment:    unionableTablePairs,
			Kunioability: unionableTablePairs[len(unionableTablePairs)-1].Sim,
			Duration:     dur,
		}

		result = append(result, QueryResult{
			TableUnion: union,
		})
	}
	response := QueryResponse{
		Result: result,
	}
	c.JSON(http.StatusOK, response)
}
