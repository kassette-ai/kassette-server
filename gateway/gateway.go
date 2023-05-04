package gateway

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/jobs"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
	enabledWriteKeysSourceMap                 map[string]string
	configSubscriberLock                      sync.RWMutex
	maxReqSize                                int
)

func loadConfig() {

	//Port where GW is running
	webPort = viper.GetInt("Gateway.webPort")
	//Number of incoming requests that are batched before initiating write
	maxBatchSize = viper.GetInt("Gateway.maxBatchSize")
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	batchTimeout = (viper.GetDuration("Gateway.batchTimeoutInMS") * time.Millisecond)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = viper.GetInt("Gateway.maxDBWriterProcess")
	// CustomVal is used as a key in the jobsDB customval column

	// Maximum request size to gateway
	maxReqSize = viper.GetInt("Gateway.maxReqSizeInKB") * 1000
}

type HandleT struct {
	webRequestQ   chan *webRequestT
	batchRequestQ chan *batchWebRequestT
	jobsDB        *jobsdb.HandleT
	ackCount      uint64
	recvCount     uint64
}

type webRequestT struct {
	request *http.Request
	writer  *http.ResponseWriter
	done    chan<- string
	reqType string
}

// Basic worker unit that works on incoming webRequests.
//
// Has three channels used to communicate between the two goroutines each worker runs.
//
// One to receive new webRequests, one to send batches of said webRequests and the third to receive errors if any in response to sending the said batches to dbWriterWorker.
type userWebRequestWorkerT struct {
	webRequestQ   chan *webRequestT
	batchRequestQ chan *batchWebRequestT
	reponseQ      chan map[uuid.UUID]string
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

// Function to batch incoming web requests
func (gateway *HandleT) webRequestBatcher() {
	var reqBuffer = make([]*webRequestT, 0)
	for {
		select {
		case req := <-gateway.webRequestQ:
			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-time.After(batchTimeout):
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		}
	}
}

func (gateway *HandleT) Setup(jobsDB *jobsdb.HandleT) {
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.startWebHandler()

}

func (gateway *HandleT) startWebHandler() {
	log.Print("Starting web handler")

	r := gin.Default()
	r.Use(gin.Recovery())

	r.POST("/collect", collect)

	CORSMiddleware()

	serverPort := viper.GetString("SERVER_PORT")

	err := r.Run(":" + serverPort)

	if err != nil {
		println(err.Error())
		return
	}
}
func collect(c *gin.Context) {

	c.JSON(200, gin.H{
		"status": "RECEIVED",
	})
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
