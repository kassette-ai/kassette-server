package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/errors"
	"kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/misc"
	"kassette.ai/kassette-server/response"
	. "kassette.ai/kassette-server/utils"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
	enabledWriteKeysSourceMap                 map[string]string
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
	ackCount      uint64
	recvCount     uint64

	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
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
			Logger.Info("Received web request")
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
	loadConfig()
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.batchRequestQ = make(chan *batchWebRequestT)

	gateway.jobsDB = jobsDB

	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, maxDBWriterProcess)

	go gateway.webRequestBatcher()

	for i := 0; i < maxDBWriterProcess; i++ {
		go gateway.webRequestBatchDBWriter(i)
	}

	gateway.startWebHandler()
}

func (gateway *HandleT) startWebHandler() {
	Logger.Info("Starting web handler")

	r := gin.Default()
	r.Use(gin.Recovery())

	//r.POST("/web", webPageHandler)
	r.POST("/extract", gateway.extractHandler)

	CORSMiddleware()

	serverPort := viper.GetString("serverPort")

	err := r.Run(":" + serverPort)

	if err != nil {
		println(err.Error())
		return
	}
}

func (gateway *HandleT) extractHandler(c *gin.Context) {
	gateway.ProcessRequest(c, "single")
}

func (gateway *HandleT) ProcessAgentRequest(payload string, writeKey string) string {
	done := make(chan string, 1)
	req := webRequestT{done: done, requestPayload: []byte(payload), writeKey: writeKey}
	gateway.webRequestQ <- &req

	errorMessage := <-done
	if errorMessage != "" {
		return "Error processing request"
	}
	return "Success"

}

func (gateway *HandleT) ProcessRequest(c *gin.Context, reqType string) {
	payload, writeKey, err := gateway.getPayloadAndWriteKey(c.Request)
	if err != nil {
		Logger.Error("Error getting payload and write key")
		return
	}
	done := make(chan string, 1)
	req := webRequestT{done: done, reqType: reqType, requestPayload: payload, writeKey: writeKey, ipAddr: c.Request.RemoteAddr, userIDHeader: c.Request.Header.Get("X-User-ID")}
	gateway.webRequestQ <- &req

	// TODO: Should wait here until response is processed
	errorMessage := <-done
	if errorMessage != "" {
		c.JSON(500, gin.H{"status": errorMessage})
		return
	}
	atomic.AddUint64(&gateway.ackCount, 1)

	c.JSON(200, gin.H{"status": respMessage})
}

func (gateway *HandleT) getPayloadAndWriteKey(r *http.Request) ([]byte, string, error) {
	//var err error
	//writeKey, _, ok := r.BasicAuth()
	//sourceID := gateway.getSourceIDForWriteKey(writeKey)
	//if !ok {
	//	println("Basic auth failed")
	//}

	writeKey := "camunda"
	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		Logger.Error("Error getting payload from request with source: " + writeKey)

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
		Logger.Error(fmt.Sprint(
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
		Logger.Info(fmt.Sprint("User Web Request Worker Started", i))
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
		var rudderId uuid.UUID
		rudderId, err = misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}
		toSet["rudderId"] = rudderId
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
		Logger.Error(fmt.Sprint("[Gateway] Failed to marshal parameters map. Parameters: %+v", params))

		marshalledParams = []byte(
			`{"error": "rudder-server gateway failed to marshal params"}`,
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
	Logger.Info(fmt.Sprint("Starting batch request DB writer, process:", process))
	for breq := range gateway.batchRequestQ {
		log.Println("Batch request received")
		var jobList []*jobsdb.JobT
		var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
		var jobWriteKeyMap = make(map[uuid.UUID]string)
		var preDbStoreCount int
		//Saving the event data read from req.request.Body to the splice.
		//Using this to send event schema to the config backend.
		var events []string
		for _, req := range breq.batchRequest {

			if req.requestPayload == nil {
				req.done <- "Request body is nil"
				preDbStoreCount++
				continue
			}
			body := req.requestPayload

			writeKey := "write_key"

			// set anonymousId if not set in payload
			var index int
			result := gjson.GetBytes(body, "batch")
			newAnonymousID := uuid.New().String()
			result.ForEach(func(_, _ gjson.Result) bool {
				if !gjson.GetBytes(body, fmt.Sprint(`batch.%v.anonymousId`, index)).Exists() {
					body, _ = sjson.SetBytes(body, fmt.Sprint(`batch.%v.anonymousId`, index), newAnonymousID)
				}
				index++
				return true // keep iterating
			})

			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
			}

			body, _ = sjson.SetBytes(body, "writeKey", writeKey)
			body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(time.RFC3339))
			events = append(events, fmt.Sprint("%s", body))

			id := uuid.New()
			//Should be function of body
			newJob := jobsdb.JobT{
				UUID:         id,
				Parameters:   []byte(fmt.Sprint(`{"source_id": "%v"}`, enabledWriteKeysSourceMap[writeKey])),
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    CustomVal,
				EventPayload: []byte(body),
			}
			jobList = append(jobList, &newJob)
			jobIDReqMap[newJob.UUID] = req
			jobWriteKeyMap[newJob.UUID] = writeKey
		}

		errorMessagesMap := gateway.jobsDB.Store(jobList)
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
