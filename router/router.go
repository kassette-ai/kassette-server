package router

import (
	"fmt"
	"time"
	"sort"
	"sync"
	"encoding/json"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/utils"
)

var (
	DB_BATCH_FETCH_SIZE			int
	MAX_PROCESS_WORKER			int

	destinationDetailMap		map[int]backendconfig.DestinationDetailT
	destinationRouterMap		map[int]DestinationRouterT
	destinationMutexMap   		map[int]*sync.RWMutex
)

func init() {
	DB_BATCH_FETCH_SIZE = 100
	MAX_PROCESS_WORKER = 10
	destinationDetailMap = map[int]backendconfig.DestinationDetailT{}
	destinationRouterMap = map[int]DestinationRouterT{}
	destinationMutexMap = map[int]*sync.RWMutex{}
}

type DestinationRouterT struct{
	DBHandle				interface{}				`json:"DBHandle"`
	Enabled					bool					`json:"Enabled"`
}

type JobProcessRequestT struct{
	JobID					int64								`json:"JobID"`
	DestinationID			int									`json:"DestinationID"`
	EventPayload			json.RawMessage						`json:"EventPayload"`
}

type JobProcessResponseT struct{
	JobID			int64					`json:"JobID"`
	State			string					`json:"State"`
	ErrorCode		string					`json:"ErrorCode"`
	ErrorResponse	json.RawMessage			`json:"ErrorResponse"`
}

type HandleT struct {
	JobsDB						*jobsdb.HandleT
	ConfigDB					*backendconfig.HandleT
	JobProcessRequestQ			chan *JobProcessRequestT
	JobProcessResponseQ			chan *JobProcessResponseT
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
			if destCatalogue.Name == "Postgres" {
				dbHandle, status := postgres.Connect(newDetail.Destination.Config)
				destinationRouterMap[destinationID] = DestinationRouterT{
					DBHandle: dbHandle,
					Enabled: status,
				}
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
		JobRequest := <-router.JobProcessRequestQ
		router.JobProcessResponseQ <- &JobProcessResponseT{
			JobID: JobRequest.JobID,
		}
	}
}

func (router *HandleT) JobsResponseWorker() {
	logger.Info(fmt.Sprintf("Router Job Response Worker started!"))
	for {
		JobResponse := <- router.JobProcessResponseQ
		logger.Info(fmt.Sprintf("JobID: %v reponsed!", JobResponse.JobID))
	}
}

func (router *HandleT) JobsRequestWorker() {
	logger.Info(fmt.Sprintf("Router Job Request Worker started!"))
	for {
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
			router.JobProcessRequestQ <- &JobProcessRequestT{
				JobID: batchEvent.JobID,
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

	for i := 0 ; i < MAX_PROCESS_WORKER ; i++ {
		go router.ProcessRouterJobs(i)
	}

	go router.JobsRequestWorker()
	go router.JobsResponseWorker()
}
