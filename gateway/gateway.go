package gateway

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"io"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/errors"
	"kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/response"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
	enabledWriteKeysSourceMap                 map[string]string
	configSubscriberLock                      sync.RWMutex
	maxReqSize                                int

	writeKeysSourceMap map[string]backendconfig.SourceT
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
	done           chan<- string
	reqType        string
	requestPayload []byte
	writeKey       string
	ipAddr         string
	userIDHeader   string
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
	gateway.webRequestQ = make(chan *webRequestT, viper.GetInt("Gateway.maxWebReqQSize"))
	gateway.startWebHandler()

}

func (gateway *HandleT) startWebHandler() {
	log.Print("Starting web handler")

	r := gin.Default()
	r.Use(gin.Recovery())

	//r.POST("/web", webPageHandler)
	r.POST("/extract", gateway.extractHandler)

	CORSMiddleware()

	serverPort := viper.GetString("SERVER_PORT")

	err := r.Run(":" + serverPort)

	if err != nil {
		println(err.Error())
		return
	}
}

func (gateway *HandleT) extractHandler(c *gin.Context) {
	gateway.ProcessRequest(c, "extract")
}

func (gateway *HandleT) ProcessRequest(c *gin.Context, reqType string) {
	payload, writeKey, err := gateway.getPayloadAndWriteKey(c.Request)
	if err != nil {
		log.Println("Error getting payload and write key")
		return
	}
	done := make(chan string, 1)
	req := webRequestT{done: done, reqType: reqType, requestPayload: payload, writeKey: writeKey, ipAddr: c.Request.RemoteAddr, userIDHeader: c.Request.Header.Get("X-User-ID")}
	gateway.webRequestQ <- &req

	// TODO: Should wait here until response is processed
	//errorMessage := <-done
	atomic.AddUint64(&gateway.ackCount, 1)
	//if errorMessage != "" {
	//	log.Printf(errorMessage)
	//	c.JSON(400, gin.H{"status": errorMessage})
	//} else {
	//	log.Printf(respMessage)
	//	c.JSON(200, gin.H{"status": respMessage})
	//}

	// Remove this when the downstream worker is complete
	log.Printf(respMessage)
	c.JSON(200, gin.H{"status": respMessage})
}

func (gateway *HandleT) getPayloadAndWriteKey(r *http.Request) ([]byte, string, error) {
	var err error
	writeKey, _, ok := r.BasicAuth()
	sourceID := gateway.getSourceIDForWriteKey(writeKey)
	if !ok {
		println("Basic auth failed")
	}
	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		println("Error getting payload from request with source: " + sourceID)

		return []byte{}, writeKey, err
	}
	return payload, writeKey, err
}

func (gateway *HandleT) getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte{}, errors.New(response.RequestBodyNil)
	}

	payload, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		log.Println(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n",
			r.Header.Get("Content-Length"),
			string(payload),
		)
		return payload, errors.New(response.RequestBodyReadFailed)
	}
	return payload, nil
}

func (*HandleT) getSourceIDForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		return writeKeysSourceMap[writeKey].ID
	}

	return ""
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
