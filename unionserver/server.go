package unionserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	ui     *UnionIndex
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
	CandTableID  string
	CandHeader   []string
	Alignment    []Pair // query to candidate table
	Kunioability float64
}

func NewServer(ui *UnionIndex) *Server {
	s := &Server{
		ui:     ui,
		router: gin.Default(),
	}
	s.router.POST("/query", s.queryHandler)
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
	result := make([]QueryResult, 0)
	queryResults := s.ui.QueryOrderAll(queryRequest.Vecs, queryRequest.N, queryRequest.K)
	for unionableTablePairs := range queryResults {
		union := Union{
			CandTableID:  unionableTablePairs[0].CandTableID,
			CandHeader:   getHeaders(unionableTablePairs[0].CandTableID, s.ui.domainDir),
			Alignment:    unionableTablePairs,
			Kunioability: unionableTablePairs[len(unionableTablePairs)-1].Sim,
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
