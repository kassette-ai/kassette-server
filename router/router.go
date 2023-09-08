package router

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/integrations/anaplan"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/integrations/powerbi"
	jobsdb "kassette.ai/kassette-server/jobs"
	stats "kassette.ai/kassette-server/services"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	DB_BATCH_FETCH_SIZE        int
	MAX_PROCESS_WORKER         int
	MAX_BATCH_PAYLOAD_SIZE     int
	BATCH_JOB_PROCESS_TIMEOUT  time.Duration
	JOB_RESPONSE_BATCH_TIMEOUT time.Duration

	destinationDetailMap     map[int]backendconfig.DestinationDetailT
	destinationRouterMap     map[int]DestinationRouterT
	destinationMutexMap      map[int]*sync.RWMutex
	destinationBatchMutexMap map[int]*sync.RWMutex
	batchJobProcessMap       map[int][]JobProcessRequestT
	lastProcessTimeMap       map[int]time.Time
	jobResponseBatch         []*jobsdb.JobStatusT
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

type DBHandleI interface {
	Connect(string) bool
	InsertPayloadInTransaction([]json.RawMessage) error
}

type RestHandleI interface {
	Init(string) bool
	Send(json.RawMessage, map[string]interface{}) (int, json.RawMessage)
}

type DestinationRouterT struct {
	DBHandle   DBHandleI   `json:"DBHandle"`
	RestHandle RestHandleI `json:"RestHandle"`
	Enabled    bool        `json:"Enabled"`
}

type JobProcessRequestT struct {
	JobID         int64           `json:"JobID"`
	DestinationID int             `json:"DestinationID"`
	EventPayload  json.RawMessage `json:"EventPayload"`
}

type JobProcessResponseT struct {
	JobID         int64           `json:"JobID"`
	State         string          `json:"State"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
}

type HandleT struct {
	JobsDB                  *jobsdb.HandleT
	ConfigDB                *backendconfig.HandleT
	JobProcessRequestQ      chan *JobProcessRequestT
	JobProcessBatchRequestQ chan *JobProcessRequestT
	JobProcessResponseQ     chan *JobProcessResponseT
	destFailure             *stats.KassetteStats
	destSuccess             *stats.KassetteStats
	destDisabled            *stats.KassetteStats
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
				Enabled:  status,
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
				Enabled:    status,
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
			var errorResponse []byte
			if !destRouter.Enabled {
				errorCode = ""
				errorResponse = []byte(`{"error": "Destination Config Disabled"}`)

			} else {
				configMap := map[string]interface{}{
					"JobID": jobRequest.JobID,
				}
				statusCodeInt, err := destRouter.RestHandle.Send(jobRequest.EventPayload, configMap)
				errorResponse = err
				errorCode = strconv.Itoa(statusCodeInt)
				if statusCodeInt != 200 && statusCodeInt != 202 {
					state = jobsdb.FailedState
				} else {
					state = jobsdb.SucceededState
				}
			}
			router.JobProcessResponseQ <- &JobProcessResponseT{
				JobID:         jobRequest.JobID,
				State:         state,
				ErrorCode:     errorCode,
				ErrorResponse: errorResponse,
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
					var errMsg string
					if destRouter.Enabled {
						payloads := []json.RawMessage{}
						for _, jobProcess := range jobProcesses {
							payloads = append(payloads, jobProcess.EventPayload)
						}
						err := destRouter.DBHandle.InsertPayloadInTransaction(payloads)
						if err != nil {
							errMsg = err.Error()
							logger.Error(errMsg)
						}
					} else {
						errMsg = "Destination NOT ENABLED"
					}
					for _, jobProcess := range jobProcesses {
						if errMsg == "" {

							router.JobProcessResponseQ <- &JobProcessResponseT{
								JobID:         jobProcess.JobID,
								State:         jobsdb.SucceededState,
								ErrorCode:     "200",
								ErrorResponse: []byte(`{"success":"OK"}`),
							}
						} else {
							router.destFailure.Increment(strconv.Itoa(jobProcess.DestinationID), 1)
							router.JobProcessResponseQ <- &JobProcessResponseT{
								JobID:         jobProcess.JobID,
								State:         jobsdb.FailedState,
								ErrorCode:     "",
								ErrorResponse: []byte(`{"error":"DB Ingestion Failed"}`),
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
		select {
		case jobResponse := <-router.JobProcessResponseQ:
			newStatus := &jobsdb.JobStatusT{
				JobID:         jobResponse.JobID,
				JobState:      jobResponse.State,
				AttemptNum:    1,
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

		if len(unprocessedList)+len(retryList) == 0 {
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
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "200",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
			destinationID := int(gjson.Get(string(batchEvent.Parameters), "destination_id").Int())
			router.JobProcessRequestQ <- &JobProcessRequestT{
				JobID:         batchEvent.JobID,
				DestinationID: destinationID,
				EventPayload:  batchEvent.EventPayload,
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

	router.destFailure = stats.NewStat("dest.failure")
	router.destSuccess = stats.NewStat("dest.success")
	router.destDisabled = stats.NewStat("dest.disabled")

	logger.Info("Router Started!")
	go BackendConfigSubscriber()

	go router.JobsRequestWorker()
	for i := 0; i < MAX_PROCESS_WORKER; i++ {
		go router.ProcessRouterJobs(i)
	}
	go router.ProcessBatchRouterJobs()
	go router.JobsResponseWorker()
}
