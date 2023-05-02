package gateway

import (
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/jobs"
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

func (gateway *HandleT) webRequestBatchDBWriter(process int) {
	//for breq := range gateway.batchRequestQ {
	//	var jobList []*jobsdb.JobT
	//	var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
	//	var jobWriteKeyMap = make(map[uuid.UUID]string)
	//	var writeKeyStats = make(map[string]int)
	//	var writeKeySuccessStats = make(map[string]int)
	//	var writeKeyFailStats = make(map[string]int)
	//	var preDbStoreCount int
	//	//Saving the event data read from req.request.Body to the splice.
	//	//Using this to send event schema to the config backend.
	//	var events []string

	//batchTimeStat.Start()
	//for _, req := range breq.batchRequest {
	//	ipAddr := misc.GetIPFromReq(req.request)
	//	if req.request.Body == nil {
	//		req.done <- "Request body is nil"
	//		preDbStoreCount++
	//		continue
	//	}
	//	body, err := ioutil.ReadAll(req.request.Body)
	//	req.request.Body.Close()
	//
	//	writeKey, _, ok := req.request.BasicAuth()
	//	if !ok {
	//		req.done <- "Failed to read writeKey from header"
	//		preDbStoreCount++
	//		misc.IncrementMapByKey(writeKeyFailStats, "noWriteKey")
	//		continue
	//	}
	//
	//	misc.IncrementMapByKey(writeKeyStats, writeKey)
	//	if err != nil {
	//		req.done <- "Failed to read body from request"
	//		preDbStoreCount++
	//		misc.IncrementMapByKey(writeKeyFailStats, writeKey)
	//		continue
	//	}
	//	if len(body) > maxReqSize {
	//		req.done <- "Request size exceeds max limit"
	//		preDbStoreCount++
	//		misc.IncrementMapByKey(writeKeyFailStats, writeKey)
	//		continue
	//	}
	//	if !gateway.isWriteKeyEnabled(writeKey) {
	//		req.done <- "Invalid Write Key"
	//		preDbStoreCount++
	//		misc.IncrementMapByKey(writeKeyFailStats, writeKey)
	//		continue
	//	}
	//
	//	// set anonymousId if not set in payload
	//	var index int
	//	result := gjson.GetBytes(body, "batch")
	//	newAnonymousID := uuid.NewV4().String()
	//	result.ForEach(func(_, _ gjson.Result) bool {
	//		if !gjson.GetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index)).Exists() {
	//			body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index), newAnonymousID)
	//		}
	//		index++
	//		return true // keep iterating
	//	})
	//
	//	if req.reqType != "batch" {
	//		body, _ = sjson.SetBytes(body, "type", req.reqType)
	//		body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
	//	}
	//
	//	logger.Debug("IP address is ", ipAddr)
	//	body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
	//	body, _ = sjson.SetBytes(body, "writeKey", writeKey)
	//	body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(time.RFC3339))
	//	events = append(events, fmt.Sprintf("%s", body))
	//
	//	id := uuid.NewV4()
	//	//Should be function of body
	//	newJob := jobsdb.JobT{
	//		UUID:         id,
	//		Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v"}`, enabledWriteKeysSourceMap[writeKey])),
	//		CreatedAt:    time.Now(),
	//		ExpireAt:     time.Now(),
	//		CustomVal:    CustomVal,
	//		EventPayload: []byte(body),
	//	}
	//	jobList = append(jobList, &newJob)
	//	jobIDReqMap[newJob.UUID] = req
	//	jobWriteKeyMap[newJob.UUID] = writeKey
	//}
	//
	//errorMessagesMap := gateway.jobsDB.Store(jobList)
	//misc.Assert(preDbStoreCount+len(errorMessagesMap) == len(breq.batchRequest))
	//for uuid, err := range errorMessagesMap {
	//	if err != "" {
	//		misc.IncrementMapByKey(writeKeyFailStats, jobWriteKeyMap[uuid])
	//	} else {
	//		misc.IncrementMapByKey(writeKeySuccessStats, jobWriteKeyMap[uuid])
	//	}
	//	jobIDReqMap[uuid].done <- err
	//}
	//
	////Sending events to config backend
	//for _, event := range events {
	//	sourcedebugger.RecordEvent(gjson.Get(event, "writeKey").Str, event)
	//}
	//
	//batchTimeStat.End()
	//batchSizeStat.Count(len(breq.batchRequest))
	//updateWriteKeyStats(writeKeyStats)
	//updateWriteKeyStatusStats(writeKeySuccessStats, true)
	//updateWriteKeyStatusStats(writeKeyFailStats, false)
}
