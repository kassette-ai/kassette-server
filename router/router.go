package router

import (
	"fmt"
	"time"
	"sort"
	"sync"
	"strconv"
	"encoding/json"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/integrations/powerbi"
	"kassette.ai/kassette-server/integrations/anaplan"
	"kassette.ai/kassette-server/integrations"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/utils"
	"github.com/tidwall/gjson"
	"github.com/google/uuid"
)

var (
	DB_BATCH_FETCH_SIZE			int
	MAX_PROCESS_WORKER			int
	MAX_BATCH_PAYLOAD_SIZE		int
	BATCH_JOB_PROCESS_TIMEOUT	time.Duration
	JOB_RESPONSE_BATCH_TIMEOUT	time.Duration

	destinationDetailMap		map[int]backendconfig.DestinationDetailT
	destinationRouterMap		map[int]DestinationRouterT
	destinationMutexMap   		map[int]*sync.RWMutex
	destinationBatchMutexMap	map[int]*sync.RWMutex
	batchJobProcessMap			map[int][]JobProcessRequestT
	lastProcessTimeMap			map[int]time.Time
	jobResponseBatch			[]*jobsdb.JobStatusT				
)

func init() {
	DB_BATCH_FETCH_SIZE = 100
	MAX_PROCESS_WORKER = 10
	MAX_BATCH_PAYLOAD_SIZE = 10
	BATCH_JOB_PROCESS_TIMEOUT = 2
	JOB_RESPONSE_BATCH_TIMEOUT = 2
	destinationDetailMap = map[int]backendconfig.DestinationDetailT{}
	destinationRouterMap = map[int]DestinationRouterT{}
	destinationMutexMap = map[int]*sync.RWMutex{}
	destinationBatchMutexMap = map[int]*sync.RWMutex{}
	batchJobProcessMap = map[int][]JobProcessRequestT{}
	lastProcessTimeMap = map[int]time.Time{}
	jobResponseBatch = []*jobsdb.JobStatusT{}
}

type DBHandleI interface{
	Connect(string) bool
	InsertPayloadInTransaction([]json.RawMessage) error
}

type RestHandleI interface{
	Init(string) bool
	Send(json.RawMessage, map[string]interface{}) (int, string, []interface{})
}

type DestinationRouterT struct{
	DBHandle				DBHandleI				`json:"DBHandle"`
	RestHandle				RestHandleI				`json:"RestHandle"`
	Enabled					bool					`json:"Enabled"`
}

type JobProcessRequestT struct{
	JobID					int64								`json:"JobID"`
	AttemptNum				int									`json:"AttemptNum"`
	DestinationID			int									`json:"DestinationID"`
	EventPayload			json.RawMessage						`json:"EventPayload"`
}

type JobProcessResponseT struct{
	JobID			int64					`json:"JobID"`
	AttemptNum		int						`json:"AttemptNum"`
	State			string					`json:"State"`
	ErrorCode		string					`json:"ErrorCode"`
	ErrorResponse	json.RawMessage			`json:"ErrorResponse"`
}

type HandleT struct {
	JobsDB						*jobsdb.HandleT
	ConfigDB					*backendconfig.HandleT
	JobProcessRequestQ			chan *JobProcessRequestT
	JobProcessBatchRequestQ		chan *JobProcessRequestT
	JobProcessResponseQ			chan *JobProcessResponseT
}

func (router *HandleT) CreateNewJobWithFailedEvents(events []interface{}, destinationID int) {
	batchPayload := integrations.BatchPayloadT{}
	batchPayload.Payload = events
	batchPayloadArr := []interface{}{batchPayload}
	destBatchPayload, _ := json.Marshal(batchPayloadArr)
	id := uuid.New()
	newJob := jobsdb.JobT{
		UUID:         id,
		Parameters:   []byte(fmt.Sprintf(`{"destination_id": "%v"}`, destinationID)),
		CreatedAt:    time.Now(),
		ExpireAt:     time.Now(),
		CustomVal:    "",
		EventPayload: destBatchPayload,
	}
	destJobs := []*jobsdb.JobT{&newJob}
	router.JobsDB.Store(destJobs)

	logger.Info(fmt.Sprintf("Storing Failed Jobs: %v", newJob))
}

func UpdateRouterConfig(connection backendconfig.ConnectionDetailT) {
	destinationID := connection.DestinationDetail.Destination.ID
	newDetail := connection.DestinationDetail
	destCatalogue := newDetail.Catalogue
	oldDetail, exist := destinationDetailMap[destinationID]
	if destCatalogue.Access == backendconfig.DestAccessType["DBPOLLING"] {
		if !exist || newDetail.Destination.Config != oldDetail.Destination.Config {
			_, exist := destinationMutexMap[destinationID]
			if !exist {
				destinationMutexMap[destinationID] = &sync.RWMutex{}
			}
			destinationMutexMap[destinationID].Lock()
			var dbHandle DBHandleI
			if destCatalogue.Name == "Postgres" {
				dbHandle = &postgres.HandleT{}
			}
			status := dbHandle.Connect(newDetail.Destination.Config)
			destinationRouterMap[destinationID] = DestinationRouterT{
				DBHandle: dbHandle,
				Enabled: status,
			}
			destinationMutexMap[destinationID].Unlock()
		}
	} else if destCatalogue.Access == backendconfig.DestAccessType["REST"] {
		if !exist || newDetail.Destination.Config != oldDetail.Destination.Config {
			_, exist := destinationMutexMap[destinationID]
			if !exist {
				destinationMutexMap[destinationID] = &sync.RWMutex{}
			}
			destinationMutexMap[destinationID].Lock()
			var restHandle RestHandleI
			if destCatalogue.Name == "PowerBI" {
				restHandle = &powerbi.HandleT{}
			} else if destCatalogue.Name == "Anaplan" {
				restHandle = &anaplan.HandleT{}
			}
			status := restHandle.Init(newDetail.Destination.Config)
			destinationRouterMap[destinationID] = DestinationRouterT{
				RestHandle: restHandle,
				Enabled: status,
			}
			destinationMutexMap[destinationID].Unlock()
		}
	}
	destinationMutexMap[destinationID].Lock()
	destinationDetailMap[destinationID] = newDetail
	destinationMutexMap[destinationID].Unlock()
}

func BackendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		detail := config.Data.(backendconfig.ConnectionDetailsT)
		for _, conn := range detail.Connections {
			go UpdateRouterConfig(conn)
		}
	}
}

func (router *HandleT) ProcessRouterJobs(index int) {
	logger.Info(fmt.Sprintf("Router Job Processor %d started!", index))
	for {
		jobRequest := <-router.JobProcessRequestQ
		destID := jobRequest.DestinationID
		logger.Info(fmt.Sprintf("Job request process for destination ID: %v", destID))
		access := destinationDetailMap[destID].Catalogue.Access
		if access == backendconfig.DestAccessType["DBPOLLING"] {
			_, ok := destinationBatchMutexMap[destID]
			if !ok {
				destinationBatchMutexMap[destID] = &sync.RWMutex{}
			}
			destinationBatchMutexMap[destID].Lock()
			batchJobProcessMap[destID] = append(batchJobProcessMap[destID], *jobRequest)
			destinationBatchMutexMap[destID].Unlock()
		} else if access == backendconfig.DestAccessType["REST"] {
			_, ok := destinationBatchMutexMap[destID]
			if !ok {
				destinationBatchMutexMap[destID] = &sync.RWMutex{}
			}
			destinationBatchMutexMap[destID].Lock()
			destRouter := destinationRouterMap[destID]
			var errorCode string
			var state string
			errMsgMap := map[string]string{}
			if !destRouter.Enabled {
				errorCode = "500"
				errMsgMap["error"] = "Destination Config Disabled"
				state = jobsdb.FailedState
			} else {
				configMap := map[string]interface{}{
					"JobID": jobRequest.JobID,
				}
				statusCodeInt, errMsg, failedJobs := destRouter.RestHandle.Send(jobRequest.EventPayload, configMap)
				logger.Info(fmt.Sprintf("heyheyhye: %v %v %v", statusCodeInt, errMsg, failedJobs))
				errorCode = strconv.Itoa(statusCodeInt)
				if statusCodeInt != 200 && statusCodeInt != 202 {
					errMsgMap["error"] = errMsg
					state = jobsdb.FailedState
				} else {
					errMsgMap["success"] = "OK"
					state = jobsdb.SucceededState
					if len(failedJobs) > 0 { 
						router.CreateNewJobWithFailedEvents(failedJobs, destID)
					}
				}
			}
			errMsgMapStr, _ := json.Marshal(errMsgMap)
			router.JobProcessResponseQ <- &JobProcessResponseT{
				JobID: jobRequest.JobID,
				AttemptNum: jobRequest.AttemptNum + 1,
				State: state,
				ErrorCode: errorCode,
				ErrorResponse: []byte(errMsgMapStr),
			}
			destinationBatchMutexMap[destID].Unlock()
		}
	}
}

func (router *HandleT) ProcessBatchRouterJobs() {
	logger.Info("Batch Router Job Processor %d started!")
	for {
		select {
		case <-time.After(BATCH_JOB_PROCESS_TIMEOUT * time.Second):
			var firstDestID int
			for destID, _ := range batchJobProcessMap {
				if firstDestID == 0 || lastProcessTimeMap[destID].Before(lastProcessTimeMap[firstDestID]) {
					firstDestID = destID
				}
			}
			if firstDestID != 0 {
				_, ok := destinationMutexMap[firstDestID]
				if !ok {
					destinationMutexMap[firstDestID] = &sync.RWMutex{}
				}
				destinationMutexMap[firstDestID].RLock()
				destinationBatchMutexMap[firstDestID].RLock()			
				destRouter := destinationRouterMap[firstDestID]
				jobProcesses := batchJobProcessMap[firstDestID]
				if destinationDetailMap[firstDestID].Catalogue.Access == backendconfig.DestAccessType["DBPOLLING"] {
					succeeded := true
					errMsgMap := map[string]string{}
					if destRouter.Enabled {
						payloads := []json.RawMessage{}
						for _, jobProcess := range jobProcesses {
							payloads = append(payloads, jobProcess.EventPayload)
						}
						err := destRouter.DBHandle.InsertPayloadInTransaction(payloads)
						if err != nil {
							errMsgMap["error"] = err.Error()
							succeeded = false
						} else {
							errMsgMap["success"] = "OK"
						}
					} else {
						errMsgMap["error"] = "Destination NOT ENABLED"
						succeeded = false
					}
					errMsgMapStr, _ := json.Marshal(errMsgMap)
					for _, jobProcess := range jobProcesses {
						if succeeded {
							router.JobProcessResponseQ <- &JobProcessResponseT{
								JobID: jobProcess.JobID,
								AttemptNum: jobProcess.AttemptNum + 1,
								State: jobsdb.SucceededState,
								ErrorCode: "200",
								ErrorResponse: []byte(errMsgMapStr),
							}
						} else {
							router.JobProcessResponseQ <- &JobProcessResponseT{
								JobID: jobProcess.JobID,
								AttemptNum: jobProcess.AttemptNum + 1,
								State: jobsdb.FailedState,
								ErrorCode: "500",
								ErrorResponse: []byte(errMsgMapStr),
							}
						}
					}
				}
				batchJobProcessMap[firstDestID] = []JobProcessRequestT{}
				lastProcessTimeMap[firstDestID] = time.Now()
				destinationBatchMutexMap[firstDestID].RUnlock()
				destinationMutexMap[firstDestID].RUnlock()
			}
		}
	}
}

func (router *HandleT) JobsResponseWorker() {
	logger.Info(fmt.Sprintf("Router Job Response Worker started!"))
	for {
		select{
		case jobResponse := <-router.JobProcessResponseQ:
			newStatus := &jobsdb.JobStatusT{
				JobID:         jobResponse.JobID,
				JobState:      jobResponse.State,
				AttemptNum:    jobResponse.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     jobResponse.ErrorCode,
				ErrorResponse: jobResponse.ErrorResponse,
			}
			jobResponseBatch = append(jobResponseBatch, newStatus)
		case <-time.After(JOB_RESPONSE_BATCH_TIMEOUT * time.Second):
			router.JobsDB.UpdateJobStatus(jobResponseBatch, []string{})
			jobResponseBatch = []*jobsdb.JobStatusT{}
		}
	}
}

func (router *HandleT) JobsRequestWorker() {
	logger.Info(fmt.Sprintf("Router Job Request Worker started!"))
	for true {
		retryList := router.JobsDB.GetToRetry([]string{}, DB_BATCH_FETCH_SIZE)
		unprocessedList := router.JobsDB.GetUnprocessed([]string{}, DB_BATCH_FETCH_SIZE)

		if len(unprocessedList) + len(retryList) == 0 {
			logger.Debug("No unprocessed or retry router jobs to process")
			time.Sleep(2 * time.Second)
			continue
		}

		combinedList := append(unprocessedList, retryList...)
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		logger.Info(fmt.Sprintf("%s", string(combinedList[0].EventPayload)))

		var statusList []*jobsdb.JobStatusT
		for _, batchEvent := range combinedList {
			newStatus := jobsdb.JobStatusT{
				JobID:         batchEvent.JobID,
				JobState:      jobsdb.ExecutingState,
				AttemptNum:    batchEvent.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
			destinationID := int(gjson.Get(string(batchEvent.Parameters), "destination_id").Int())
			router.JobProcessRequestQ <- &JobProcessRequestT{
				JobID: batchEvent.JobID,
				AttemptNum: batchEvent.LastJobStatus.AttemptNum,
				DestinationID: destinationID,
				EventPayload: batchEvent.EventPayload,
			}
		}
		router.JobsDB.UpdateJobStatus(statusList, []string{})
	}
}

func (router *HandleT) Setup(jobsDB *jobsdb.HandleT, configDB *backendconfig.HandleT) {
	router.JobsDB = jobsDB
	router.ConfigDB = configDB
	router.JobProcessRequestQ = make(chan *JobProcessRequestT, MAX_PROCESS_WORKER)
	router.JobProcessResponseQ = make(chan *JobProcessResponseT, MAX_PROCESS_WORKER)
	
	logger.Info("Router Started!")
	go BackendConfigSubscriber()

	go router.JobsRequestWorker()
	for i := 0 ; i < MAX_PROCESS_WORKER ; i++ {
		go router.ProcessRouterJobs(i)
	}
	go router.ProcessBatchRouterJobs()
	go router.JobsResponseWorker()
}
