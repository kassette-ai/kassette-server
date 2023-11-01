package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go"
	stats "kassette.ai/kassette-server/services"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/errors"
	"kassette.ai/kassette-server/integrations/anaplan"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/integrations/powerbi"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/misc"
	"kassette.ai/kassette-server/response"
	"kassette.ai/kassette-server/sources"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"

	"golang.org/x/sync/errgroup"
)

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
	enabledWriteKeysSourceMap                 map[string]int
	connectionsMap                            map[string][]int
	configSubscriberLock                      sync.RWMutex
	maxReqSize                                int

	maxDBBatchSize, maxHeaderBytes, maxConcurrentRequests int

	enabledWriteKeyWorkspaceMap map[string]string

	writeKeysSourceMap    map[string]backendconfig.SourceT
	userWebRequestWorkers []*userWebRequestWorkerT

	maxUserWebRequestWorkerProcess int

	allowReqsWithoutUserIDAndAnonymousID bool

	maxUserWebRequestBatchSize int

	// Checks a valid semver supplied
	semverRegexp = regexp.MustCompile(`^v?([0-9]+)(\.[0-9]+)?(\.[0-9]+)?(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?$`)

	// JWT Secret Key
	secretKey string
)

func loadConfig() {

	//Port where GW is running
	webPort = viper.GetInt("gateway.webPort")
	//Number of incoming requests that are batched before initiating write
	maxBatchSize = viper.GetInt("gateway.maxBatchSize")
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	batchTimeout = (viper.GetDuration("gateway.batchTimeoutInMS") * time.Millisecond)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = viper.GetInt("gateway.maxDBWriterProcess")
	// CustomVal is used as a key in the jobsDB customval column

	// Maximum request size to gateway
	maxReqSize = viper.GetInt("gateway.maxReqSizeInKB") * 1000

	allowReqsWithoutUserIDAndAnonymousID = viper.GetBool("gateway.allowReqsWithoutUserIDAndAnonymousID")

	secretKey = viper.GetString("secretKey")
}

type userWorkerBatchRequestT struct {
	jobList     []*jobsdb.JobT
	respChannel chan map[uuid.UUID]string
}

type batchUserWorkerBatchRequestT struct {
	batchUserWorkerBatchRequest []*userWorkerBatchRequestT
}

type HandleT struct {
	webRequestQ   chan *webRequestT
	batchRequestQ chan *batchWebRequestT
	jobsDB        *jobsdb.HandleT
	routerJobsDB  *jobsdb.HandleT
	configDB      *backendconfig.HandleT
	ackCount      uint64
	recvCount     uint64

	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
	sourceSuccess                *stats.KassetteStats
	sourceFailure                *stats.KassetteStats
	sourceDisabled               *stats.KassetteStats
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
			logger.Info("Received web request")
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-time.After(batchTimeout):
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = make([]*webRequestT, 0)
			}
		}
	}
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		enabledWriteKeysSourceMap = map[string]int{}
		connectionsMap = map[string][]int{}
		detail := config.Data.(backendconfig.ConnectionDetailsT)
		for _, conn := range detail.Connections {
			source := conn.SourceDetail.Source
			connection := conn.Connection
			if source.Enabled() {
				enabledWriteKeysSourceMap[source.WriteKey] = source.ID
				connectionsMap[source.WriteKey] = append(connectionsMap[source.WriteKey], connection.ID)
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (gateway *HandleT) Setup(jobsDB *jobsdb.HandleT, routerJobsDB *jobsdb.HandleT, configDB *backendconfig.HandleT) {
	loadConfig()
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.batchRequestQ = make(chan *batchWebRequestT)

	gateway.jobsDB = jobsDB
	gateway.routerJobsDB = routerJobsDB
	gateway.configDB = configDB

	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, maxDBWriterProcess)

	gateway.sourceSuccess = stats.NewStat("source.success")
	gateway.sourceFailure = stats.NewStat("source.failure")
	gateway.sourceSuccess = stats.NewStat("source.disabled")

	go gateway.webRequestBatcher()

	for i := 0; i < maxDBWriterProcess; i++ {
		go gateway.webRequestBatchDBWriter(i)
	}

	go backendConfigSubscriber()

	gateway.startWebHandler()
}

func (gateway *HandleT) startWebHandler() {
	logger.Info("Starting web handler")

	r := gin.Default()
	r.Use(gin.Recovery())
	r.Use(cors.Default())
	r.Static("/static", "./static")

	r.POST("/extract", gateway.extractHandler)

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	r.GET("/service-catalogue", func(c *gin.Context) {
		service_type := c.Query("type")
		c.JSON(http.StatusOK, gateway.configDB.GetServiceCatalogue(service_type))
	})

	r.GET("/service-catalogue/:id", func(c *gin.Context) {
		service_id_str := c.Param("id")
		service_id, err := strconv.Atoi(service_id_str)
		catalogue, err := gateway.configDB.GetServiceCatalogueByID(service_id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"Error": err.Error()})
			bugsnag.Notify(err)
		} else {
			c.JSON(http.StatusOK, catalogue)
		}
	})
	// Service Catalogue moved to yaml
	// r.POST("/service-catalogue", func(c *gin.Context) {

	// 	var catalogue backendconfig.ServiceCatalogueT
	// 	catalogue.Name = c.PostForm("name")
	// 	catalogue.Type = c.PostForm("type")
	// 	catalogue.Access = c.PostForm("access")
	// 	catalogue.Category = c.PostForm("category")
	// 	catalogue.Url = c.PostForm("url")
	// 	catalogue.Notes = c.PostForm("notes")
	// 	catalogue.MetaData = c.PostForm("metadata")

	// 	file, err := c.FormFile("icon")
	// 	if err != nil {
	// 		c.JSON(http.StatusBadRequest, gin.H{"Error": "No Valid File"})
	// 		return
	// 	}

	// 	destIconPath, err := misc.UploadFile("static/icons/", file)
	// 	if err != nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{"Error": err.Error()})
	// 		return
	// 	}
	// 	catalogue.IconUrl = destIconPath

	// 	success := gateway.configDB.CreateNewServiceCatalogue(catalogue)
	// 	c.JSON(http.StatusOK, gin.H{"success": success})
	// })

	// r.DELETE("/service-catalogue/:id", func(c *gin.Context) {
	// 	service_id := c.Param("id")
	// 	success := gateway.configDB.DeleteServiceCatalogue(service_id)
	// 	c.JSON(http.StatusOK, gin.H{
	// 		"success": success,
	// 	})
	// })

	r.GET("/source", func(c *gin.Context) {
		c.JSON(http.StatusOK, gateway.configDB.GetAllSources())
	})

	r.GET("/source/:id", func(c *gin.Context) {
		source_id_str := c.Param("id")
		source_id, err := strconv.Atoi(source_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			sourceDetail, err := gateway.configDB.GetSourceDetailByID(source_id)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			}
			c.JSON(http.StatusOK, sourceDetail)
		}
	})

	r.POST("/source", func(c *gin.Context) {
		var source backendconfig.SourceInstanceT
		var writeKeyPayload misc.WriteKeyPayloadT

		err := c.BindJSON(&source)
		if err != nil {
			logger.Error(fmt.Sprintf("Error occured while unmarshaling json data. Error: %s", err.Error()))
			c.JSON(http.StatusBadRequest, err.Error())
			return
		}

		writeKeyPayload.CustomerName = source.CustomerName
		writeKeyPayload.SecretKey = source.SecretKey
		source.WriteKey = misc.GenerateWriteKey(writeKeyPayload)

		success := gateway.configDB.CreateNewSource(source)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.PATCH("/source", func(c *gin.Context) {
		var source backendconfig.SourceInstanceT
		var writeKeyPayload misc.WriteKeyPayloadT
		err := c.BindJSON(&source)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		if source.CustomerName != "" && source.SecretKey != "" {
			writeKeyPayload.CustomerName = source.CustomerName
			writeKeyPayload.SecretKey = source.SecretKey
			source.WriteKey = misc.GenerateWriteKey(writeKeyPayload)
		}

		success := gateway.configDB.UpdateSource(source)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.DELETE("/source/:id", func(c *gin.Context) {
		source_id_str := c.Param("id")
		source_id, err := strconv.Atoi(source_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		} else {
			success := gateway.configDB.DeleteSource(source_id)
			c.JSON(http.StatusOK, gin.H{"success": success})
		}
	})

	r.GET("/destination", func(c *gin.Context) {
		c.JSON(http.StatusOK, gateway.configDB.GetAllDestinations())
	})

	r.GET("/destination/:id", func(c *gin.Context) {
		destination_id_str := c.Param("id")
		destination_id, err := strconv.Atoi(destination_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			destinationDetail, err := gateway.configDB.GetDestinationDetailByID(destination_id)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			}
			c.JSON(http.StatusOK, destinationDetail)
		}
	})

	r.POST("/destination", func(c *gin.Context) {
		var destination backendconfig.DestinationInstanceT
		err := c.BindJSON(&destination)
		if err != nil {
			logger.Error(fmt.Sprintf("Error occured while unmarshaling json data. Error: %s", err.Error()))
			c.JSON(http.StatusBadRequest, err.Error())
		}
		success := gateway.configDB.CreateNewDestination(destination)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.PATCH("/destination", func(c *gin.Context) {
		var destination backendconfig.DestinationInstanceT
		err := c.BindJSON(&destination)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		success := gateway.configDB.UpdateDestination(destination)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.DELETE("/destination/:id", func(c *gin.Context) {
		destination_id_str := c.Param("id")
		destination_id, err := strconv.Atoi(destination_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		} else {
			success := gateway.configDB.DeleteDestination(destination_id)
			c.JSON(http.StatusOK, gin.H{"success": success})
		}
	})

	r.GET("/connection", func(c *gin.Context) {
		c.JSON(http.StatusOK, gateway.configDB.GetAllConnections())
	})

	r.GET("/connection/:id", func(c *gin.Context) {
		connection_id_str := c.Param("id")
		connection_id, err := strconv.Atoi(connection_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		} else {
			connection, err := gateway.configDB.GetConnectionByID(connection_id)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			}
			c.JSON(http.StatusOK, connection)
		}
	})

	r.POST("/connection", func(c *gin.Context) {
		var connection backendconfig.ConnectionInstanceT
		err := c.BindJSON(&connection)
		if err != nil {
			logger.Error(fmt.Sprintf("Error occured while unmarshaling json data. Error: %s", err.Error()))
			c.JSON(http.StatusBadRequest, err.Error())
		}
		success := gateway.configDB.CreateNewConnection(connection)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.PATCH("/connection", func(c *gin.Context) {
		var connection backendconfig.ConnectionInstanceT
		err := c.BindJSON(&connection)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		success := gateway.configDB.UpdateConnection(connection)
		c.JSON(http.StatusOK, gin.H{"success": success})
	})

	r.DELETE("/connection/:id", func(c *gin.Context) {
		connection_id_str := c.Param("id")
		connection_id, err := strconv.Atoi(connection_id_str)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		} else {
			success := gateway.configDB.DeleteConnection(connection_id)
			c.JSON(http.StatusOK, gin.H{"success": success})
		}
	})

	r.POST("/authenticate", func(c *gin.Context) {
		var payload misc.WriteKeyPayloadT
		c.BindJSON(&payload)
		hashValue := misc.GenerateWriteKey(payload)
		passed, err := gateway.configDB.Authenticate(hashValue)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"Error": err.Error()})
		} else {
			if passed {
				c.JSON(http.StatusOK, gin.H{"Status": "Authentication Passed"})
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"Status": "No matched source instance!"})
			}
		}
	})

	r.GET("/field-options", func(c *gin.Context) {
		cataType := c.Query("type")
		name := c.Query("name")
		if cataType == "destination" {
			switch name {
			case "Postgres":
				c.JSON(http.StatusOK, postgres.TypeMapKassetteToDest)
			case "PowerBI":
				c.JSON(http.StatusOK, powerbi.TypeMapKassetteToDest)
			case "Anaplan":
				c.JSON(http.StatusOK, anaplan.TypeMapKassetteToDest)
			}
		} else if cataType == "source" {
			c.JSON(http.StatusOK, sources.TypeMapKassetteToSrc)
		}
	})

	r.GET("/router-job-status", func(c *gin.Context) {
		c.JSON(http.StatusOK, gateway.routerJobsDB.GetJobHealth())
	})

	serverPort := viper.GetString("serverPort")

	err := r.Run(":" + serverPort)

	if err != nil {
		println(err.Error())
		return
	}
}

func (gateway *HandleT) extractHandler(c *gin.Context) {
	gateway.ProcessRequest(c, "batch")
}

func (gateway *HandleT) ProcessAgentRequest(payload string, writeKey string) string {
	done := make(chan string, 1)
	req := webRequestT{done: done, requestPayload: []byte(payload), writeKey: writeKey}
	gateway.webRequestQ <- &req

	errorMessage := <-done
	if errorMessage != "" {
		logger.Error(fmt.Sprint("Error processing request: ", errorMessage))
		return "Error processing request"
	}
	return "Success"

}

func (gateway *HandleT) ProcessRequest(c *gin.Context, reqType string) {
	payload, writeKey, err := gateway.getPayloadAndWriteKey(c.Request)
	if err != nil {
		bugsnag.Notify(err)
		logger.Error(fmt.Sprintf("Error getting payload and write key. Error: %s", err.Error()))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	done := make(chan string, 1)
	req := webRequestT{
		done:           done,
		reqType:        reqType,
		requestPayload: payload,
		writeKey:       writeKey,
		ipAddr:         c.Request.RemoteAddr,
		userIDHeader:   c.Request.Header.Get("X-User-ID"),
	}
	gateway.webRequestQ <- &req

	// TODO: Should wait here until response is processed
	errorMessage := <-done
	if errorMessage != "" {
		logger.Error(fmt.Sprint("Error processing request: ", errorMessage))
		c.JSON(500, gin.H{"status": errorMessage})
		return
	}
	atomic.AddUint64(&gateway.ackCount, 1)

	c.JSON(200, gin.H{"status": "ack"})
}

func (gateway *HandleT) getPayloadAndWriteKey(r *http.Request) ([]byte, string, error) {
	var err error
	var writeKeyPayload misc.WriteKeyPayloadT
	kassetteHeader := r.Header.Get("Authorization")
	authParts := strings.SplitN(kassetteHeader, " ", 2)
	if len(authParts) != 2 || authParts[0] != "Basic" {
		return []byte{}, "", errors.New("Invalid authorization header format")
	}
	decodedKassetteHeader, err := base64.StdEncoding.DecodeString(authParts[1])
	if err != nil {
		return []byte{}, "", errors.New("Not valid base64 encoded string")
	}
	splittedDecode := strings.SplitN(string(decodedKassetteHeader), ":", 2)
	if len(splittedDecode) != 2 {
		return []byte{}, "", errors.New("Not valid customer-key format")
	}
	writeKeyPayload.CustomerName = splittedDecode[0]
	writeKeyPayload.SecretKey = splittedDecode[1]

	writeKey := misc.GenerateWriteKey(writeKeyPayload)
	passed, err := gateway.configDB.Authenticate(writeKey)

	if err != nil {
		return []byte{}, "", nil
	}

	if !passed {
		return []byte{}, "", errors.New("No valid writeKey")
	}

	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		logger.Error("Error getting payload from request with source: " + writeKey)

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
		logger.Error(fmt.Sprint(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n",
			r.Header.Get("Content-Length"),
			string(payload)),
		)
		return payload, errors.New(response.RequestBodyReadFailed)
	}
	return payload, nil
}

func (*HandleT) isValidWriteKey(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	_, ok := writeKeysSourceMap[writeKey]
	return ok
}

func (gateway *HandleT) runUserWebRequestWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for _, y := range userWebRequestWorkers {
		userWebRequestWorker := y
		g.Go(func() error {
			gateway.userWebRequestWorkerProcess(userWebRequestWorker)
			return nil
		})

	}
	_ = g.Wait()

	close(gateway.userWorkerBatchRequestQ)
}

func (gateway *HandleT) initUserWebRequestWorkers() {
	userWebRequestWorkers = make([]*userWebRequestWorkerT, maxUserWebRequestWorkerProcess)
	for i := 0; i < maxUserWebRequestWorkerProcess; i++ {
		logger.Info(fmt.Sprint("User Web Request Worker Started", i))
		userWebRequestWorker := &userWebRequestWorkerT{
			webRequestQ:   make(chan *webRequestT, maxUserWebRequestBatchSize),
			batchRequestQ: make(chan *batchWebRequestT),
			reponseQ:      make(chan map[uuid.UUID]string),
		}
		userWebRequestWorkers[i] = userWebRequestWorker
	}
}

var (
	errRequestDropped    = errors.New("request dropped")
	errRequestSuppressed = errors.New("request suppressed")
)

//	Listens on the `batchRequestQ` channel of the webRequestWorker for new batches of webRequests
//	Goes over the webRequests in the batch and filters them out(`rateLimit`, `maxReqSize`).
//	And creates a `jobList` which is then sent to `userWorkerBatchRequestQ` of the gateway and waits for a response
//	from the `dbwriterWorker`s that batch them and write to the db.
//
// Finally sends responses(error) if any back to the webRequests over their `done` channels
func (gateway *HandleT) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
	for breq := range userWebRequestWorker.batchRequestQ {
		var jobList []*jobsdb.JobT
		jobIDReqMap := make(map[uuid.UUID]*webRequestT)
		jobSourceTagMap := make(map[uuid.UUID]string)

		// Saving the event data read from req.request.Body to the splice.
		// Using this to send event schema to the config backend.
		var eventBatchesToRecord []sourceDebugger
		//batchStart := time.Now()
		for _, req := range breq.batchRequest {
			writeKey := req.writeKey
			sourceTag := gateway.getSourceTagFromWriteKey(writeKey)

			jobData, err := gateway.getJobDataFromRequest(req)

			if err != nil {
				switch {
				case err == errRequestDropped:
					req.done <- response.TooManyRequests

				case err == errRequestSuppressed:
					req.done <- "" // no error

				default:
					req.done <- err.Error()

				}
				continue
			}
			jobList = append(jobList, jobData.job)
			jobIDReqMap[jobData.job.UUID] = req
			jobSourceTagMap[jobData.job.UUID] = sourceTag
			eventBatchesToRecord = append(eventBatchesToRecord, sourceDebugger{data: jobData.job.EventPayload, writeKey: writeKey})
		}

	}
}

type sourceDebugger struct {
	data     []byte
	writeKey string
}

func (gateway *HandleT) getSourceTagFromWriteKey(writeKey string) string {
	sourceName := gateway.getSourceNameForWriteKey(writeKey)
	sourceTag := misc.GetTagName(writeKey, sourceName)
	return sourceTag
}

func (*HandleT) getSourceNameForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		return writeKeysSourceMap[writeKey].Name
	}

	return "-notFound-"
}

func (*HandleT) getSourceIDForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		return writeKeysSourceMap[writeKey].ID
	}

	return ""
}

func (gateway *HandleT) getJobDataFromRequest(req *webRequestT) (jobData *jobFromReq, err error) {
	var (
		writeKey = req.writeKey
		sourceID = gateway.getSourceIDForWriteKey(writeKey)
		// Should be function of body
		workspaceId  = gateway.getWorkspaceForWriteKey(writeKey)
		userIDHeader = req.userIDHeader
		ipAddr       = req.ipAddr
		body         = req.requestPayload
	)

	jobData = &jobFromReq{}
	if !gjson.ValidBytes(body) {
		err = errors.New(response.InvalidJSON)
		return
	}

	if req.reqType != "batch" {
		body, err = sjson.SetBytes(body, "type", req.reqType)
		if err != nil {
			err = errors.New(response.NotKassetteEvent)
			return
		}
		body, _ = sjson.SetRawBytes(BatchEvent, "batch.0", body)
	}

	eventsBatch := gjson.GetBytes(body, "batch").Array()
	jobData.numEvents = len(eventsBatch)

	if !gateway.isValidWriteKey(writeKey) {
		err = errors.New(response.InvalidWriteKey)
		return
	}

	if !gateway.isWriteKeyEnabled(writeKey) {
		err = errors.New(response.SourceDisabled)
		return
	}

	var (
		// map to hold modified/filtered events of the batch
		out []map[string]interface{}

		// values retrieved from first event in batch
		firstUserID                                 string
		firstSourcesJobRunID, firstSourcesTaskRunID string

		// facts about the batch populated as we iterate over events
		containsAudienceList bool
	)

	for idx, v := range eventsBatch {
		toSet, ok := v.Value().(map[string]interface{})
		if !ok {
			err = errors.New(response.NotKassetteEvent)
			return
		}

		anonIDFromReq := strings.TrimSpace(misc.GetStringifiedData(toSet["anonymousId"]))
		userIDFromReq := strings.TrimSpace(misc.GetStringifiedData(toSet["userId"]))
		eventTypeFromReq, _ := misc.MapLookup(
			toSet,
			"type",
		).(string)

		if isNonIdentifiable(anonIDFromReq, userIDFromReq, eventTypeFromReq) {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}

		if idx == 0 {
			firstUserID = buildUserID(userIDHeader, anonIDFromReq, userIDFromReq)
			firstSourcesJobRunID, _ = misc.MapLookup(
				toSet,
				"context",
				"sources",
				"job_run_id",
			).(string)
			firstSourcesTaskRunID, _ = misc.MapLookup(
				toSet,
				"context",
				"sources",
				"task_run_id",
			).(string)

			// calculate version
			firstSDKName, _ := misc.MapLookup(
				toSet,
				"context",
				"library",
				"name",
			).(string)
			firstSDKVersion, _ := misc.MapLookup(
				toSet,
				"context",
				"library",
				"version",
			).(string)
			if firstSDKVersion != "" && !semverRegexp.Match([]byte(firstSDKVersion)) { // skipcq: CRT-A0007
				firstSDKVersion = "invalid"
			}
			if firstSDKName != "" || firstSDKVersion != "" {
				jobData.version = firstSDKName + "/" + firstSDKVersion
			}
		}

		//if isUserSuppressed(workspaceId, userIDFromReq, sourceID) {
		//	suppressed = true
		//	continue
		//}

		// hashing combination of userIDFromReq + anonIDFromReq, using colon as a delimiter
		var kassetteId uuid.UUID
		kassetteId, err = misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}
		toSet["kassetteId"] = kassetteId
		setRandomMessageIDWhenEmpty(toSet)
		if eventTypeFromReq == "audiencelist" {
			containsAudienceList = true
		}

		out = append(out, toSet)
	}

	//if len(out) == 0 && suppressed {
	//	err = errRequestSuppressed
	//	return
	//}

	if len(body) > maxReqSize && !containsAudienceList {
		err = errors.New(response.RequestBodyTooLarge)
		return
	}

	body, _ = sjson.SetBytes(body, "batch", out)
	body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
	body, _ = sjson.SetBytes(body, "writeKey", writeKey)
	body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(misc.RFC3339Milli))

	id := uuid.New()

	params := map[string]interface{}{
		"source_id":          sourceID,
		"source_job_run_id":  firstSourcesJobRunID,
		"source_task_run_id": firstSourcesTaskRunID,
	}
	marshalledParams, err := json.Marshal(params)
	if err != nil {
		logger.Error(fmt.Sprint("[Gateway] Failed to marshal parameters map. Parameters: %+v", params))

		marshalledParams = []byte(
			`{"error": "kassette-server gateway failed to marshal params"}`,
		)
	}
	err = nil
	job := &jobsdb.JobT{
		UUID:         id,
		UserID:       firstUserID,
		Parameters:   marshalledParams,
		CustomVal:    CustomVal,
		EventPayload: body,
		EventCount:   jobData.numEvents,
		WorkspaceId:  workspaceId,
	}
	jobData.job = job
	return
}

var CustomVal string

const (
	DELIMITER                 = string("<<>>")
	eventStreamSourceCategory = "eventStream"
	extractEvent              = "extract"
)

var BatchEvent = []byte(`
	{
		"batch": [
		]
	}
`)

func (*HandleT) isWriteKeyEnabled(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	return writeKeysSourceMap[writeKey].Enabled
}

func (*HandleT) getWorkspaceForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := enabledWriteKeyWorkspaceMap[writeKey]; ok {
		return enabledWriteKeyWorkspaceMap[writeKey]
	}
	return ""
}

type jobFromReq struct {
	job       *jobsdb.JobT
	numEvents int
	version   string
}

func GetStringifiedData(data interface{}) string {
	if data == nil {
		return ""
	}
	switch d := data.(type) {
	case string:
		return d
	default:
		dataBytes, err := json.Marshal(d)
		if err != nil {
			return fmt.Sprint(d)
		}
		return string(dataBytes)
	}
}

func isNonIdentifiable(anonIDFromReq, userIDFromReq, eventType string) bool {
	if eventType == extractEvent {
		// extract event is allowed without user id and anonymous id
		return false
	}
	if anonIDFromReq == "" && userIDFromReq == "" {
		return !allowReqsWithoutUserIDAndAnonymousID
	}
	return false
}

func buildUserID(userIDHeader, anonIDFromReq, userIDFromReq string) string {
	if anonIDFromReq != "" {
		return userIDHeader + DELIMITER + anonIDFromReq + DELIMITER + userIDFromReq
	}
	return userIDHeader + DELIMITER + userIDFromReq + DELIMITER + userIDFromReq
}

// checks for the presence of messageId in the event
//
// sets to a new uuid if not present
func setRandomMessageIDWhenEmpty(event map[string]interface{}) {
	messageID, _ := event["messageId"].(string)
	if strings.TrimSpace(messageID) == "" {
		event["messageId"] = uuid.New().String()
	}
}

// The  writer of the batch request to the DB.
// Listens on the channel and sends the done response back
func (gateway *HandleT) webRequestBatchDBWriter(process int) {
	logger.Info(fmt.Sprint("Starting batch request DB writer, process:", process))
	for breq := range gateway.batchRequestQ {
		log.Println("Batch request received")
		var jobList []*jobsdb.JobT
		var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
		var jobWriteKeyMap = make(map[uuid.UUID]string)
		var preDbStoreCount int
		for _, req := range breq.batchRequest {
			if req.requestPayload == nil {
				req.done <- "Request body is nil"
				preDbStoreCount++
				continue
			}
			body := req.requestPayload
			writeKey := req.writeKey
			ipAddr := req.ipAddr

			newAnonymousID := uuid.New().String()
			batchReqs := gjson.GetBytes(body, "batch")
			batchReqs.ForEach(func(index, req gjson.Result) bool {
				body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index), newAnonymousID)
				return true // keep iterating
			})

			body, _ = sjson.SetBytes(body, "writeKey", writeKey)
			body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
			body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(time.RFC3339))

			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
			}

			connectionIDStr := []string{}
			for _, connectionID := range connectionsMap[writeKey] {
				connectionIDStr = append(connectionIDStr, fmt.Sprintf("%d", connectionID))
			}

			id := uuid.New()
			newJob := jobsdb.JobT{
				UUID:         id,
				Parameters:   []byte(fmt.Sprintf(`{"connection_id": [%v]}`, strings.Join(connectionIDStr, ","))),
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    CustomVal,
				EventPayload: body,
			}

			jobList = append(jobList, &newJob)
			jobIDReqMap[newJob.UUID] = req
			jobWriteKeyMap[newJob.UUID] = writeKey
		}

		errorMessagesMap, _ := gateway.jobsDB.Store(jobList)
		for uuid, err := range errorMessagesMap {
			jobIDReqMap[uuid].done <- err
		}
	}
}

var batchEvent = []byte(`
	{
		"batch": [
		]
	}
`)
