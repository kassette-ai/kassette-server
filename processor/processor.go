package processor

import (
	"encoding/json"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/misc"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/integrations"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/integrations/powerbi"
	"kassette.ai/kassette-server/integrations/anaplan"
	"kassette.ai/kassette-server/sources"
	"kassette.ai/kassette-server/sources/camunda"
	"log"
	"sort"
	"sync"
	"time"
)

var (
	processSessions bool

	sessionThresholdInS    time.Duration
	sessionThresholdEvents int

	configSubscriberLock   sync.RWMutex
	configInitialized	   bool
	connectionMap 		   map[int]backendconfig.ConnectionDetailT
	rawDataDestinations    []string

	transformBatchSize int
)

type HandleT struct {
	gatewayDB      *jobsdb.HandleT
	routerDB       *jobsdb.HandleT
	batchRouterDB  *jobsdb.HandleT
	transformer    *transformerHandleT
	userJobListMap map[string][]*jobsdb.JobT
	userEventsMap  map[string][]interface{}
	userPQItemMap  map[string]*pqItemT
	userJobPQ      pqT
	userPQLock     sync.Mutex
}

func loadConfig() {
	processSessions = true
	rawDataDestinations = []string{"S3"}
	transformBatchSize = 10
}

// Setup initializes the module
func (proc *HandleT) Setup(gatewayDB *jobsdb.HandleT, routerDB *jobsdb.HandleT, batchRouterDB *jobsdb.HandleT) {
	loadConfig()

	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.transformer = &transformerHandleT{}
	proc.userJobListMap = make(map[string][]*jobsdb.JobT)
	proc.userEventsMap = make(map[string][]interface{})
	proc.userPQItemMap = make(map[string]*pqItemT)
	proc.userJobPQ = make(pqT, 0)

	configInitialized = false

	proc.transformer.Setup()

	go backendConfigSubscriber()
	go proc.mainLoop()

	if processSessions {
		logger.Info("Starting session processor")
		go proc.createSessions()
	}
}

func (proc *HandleT) mainLoop() {

	for {

		if !configInitialized {
			logger.Info("Backend Config is not initialised yet!")
			continue
		}

		toQuery := 10

		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			logger.Debug("No unprocessed and retry jobs to process")
			time.Sleep(2 * time.Second)
			continue
		}

		combinedList := append(unprocessedList, retryList...)

		//Sort by JOBID
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		if processSessions {
			//Mark all as executing so next query doesn't pick it up
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
			}
			proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
			proc.addJobsToSessions(combinedList)
		} else {
			logger.Info("Processing jobs for destinations")
			proc.processJobsForDest(combinedList, nil)
		}
		//
	}
}

func (proc *HandleT) processJobsForDest(jobList []*jobsdb.JobT, parsedEventList [][]interface{}) {

	var destJobs []*jobsdb.JobT
	var batchDestJobs []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var eventsByDest = make(map[int][]interface{})

	//Each block we receive from a client has a bunch of
	//requests. We parse the block and take out individual
	//requests, call the destination specific transformation
	//function and create jobs for them.
	//Transformation is called for a batch of jobs at a time
	//to speed-up execution.

	//Event count for performance stat monitoring
	
	totalEvents := 0

	for idx, batchEvent := range jobList {

		var eventList []interface{}
		var ok bool

		if parsedEventList == nil {
			eventList, ok = misc.ParseKassetteEventBatch(batchEvent.EventPayload)
		} else {
			eventList = parsedEventList[idx]
			ok = (eventList != nil)
		}

		requestIP := gjson.Get(string(batchEvent.EventPayload), "requestIP").Str
		connectionIDArr := gjson.Get(string(batchEvent.Parameters), "connection_id").Array()
		receivedAt := gjson.Get(string(batchEvent.EventPayload), "receivedAt").Time()

		if ok {
			for _, connectionIDResult := range connectionIDArr {
				connectionID := int(connectionIDResult.Int())
				configSubscriberLock.RLock()
				connection := connectionMap[connectionID]
				configSubscriberLock.RUnlock()
				if !connection.DestinationDetail.Destination.Enabled() {
					logger.Info(fmt.Sprintf("Destination %v is disabled.", connection.DestinationDetail.Destination.ID))
					continue
				}
				//Iterate through all the events in the batch
				for _, singularEvent := range eventList {
					//We count this as one, not destination specific ones
					totalEvents++
					//Getting all the destinations which are enabled for this
					//event

					shallowEventCopy := make(map[string]interface{})
					singularEventMap, _ := singularEvent.(map[string]interface{})

					// set timestamp skew based on timestamp fields from SDKs
					originalTimestamp := getTimestampFromEvent(singularEventMap, "originalTimestamp")
					sentAt := getTimestampFromEvent(singularEventMap, "sentAt")

					shallowEventCopy["message"] = singularEventMap
					shallowEventCopy["request_ip"] = requestIP

					// set all timestamps in RFC3339 format
					shallowEventCopy["receivedAt"] = receivedAt.Format(time.RFC3339)
					shallowEventCopy["originalTimestamp"] = originalTimestamp.Format(time.RFC3339)
					shallowEventCopy["sentAt"] = sentAt.Format(time.RFC3339)
					shallowEventCopy["timestamp"] = receivedAt.Add(-sentAt.Sub(originalTimestamp)).Format(time.RFC3339)

					//We have at-least one event so marking it good
					_, ok = eventsByDest[connectionID]
					if !ok {
						eventsByDest[connectionID] = make([]interface{}, 0)
					}
					eventsByDest[connectionID] = append(eventsByDest[connectionID], shallowEventCopy)
				}
			}
		} else {
			logger.Info("Error parsing event batch, possible invalid event list (empty or not JSON)")
		}

		//Mark the batch event as processed
		newStatus := jobsdb.JobStatusT{
			JobID:         batchEvent.JobID,
			JobState:      jobsdb.SucceededState,
			AttemptNum:    batchEvent.LastJobStatus.AttemptNum + 1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":"OK"}`),
		}
		statusList = append(statusList, &newStatus)
	}

	//Now do the actual transformation. We call it in batches, once
	//for each destination ID
	for connectionID, destEventList := range eventsByDest {
		//Call transform for this destination. Returns
		//the JSON we can send to the destination

		logger.Info(fmt.Sprintf("Transform input size: %d", len(destEventList)))
		configSubscriberLock.RLock()
		transformRule := connectionMap[connectionID].Connection.Transforms
		srcConfigData := connectionMap[connectionID].SourceDetail.Source.Config
		srcCatalogueName := connectionMap[connectionID].SourceDetail.Catalogue.Name
		destConfigData := connectionMap[connectionID].DestinationDetail.Destination.Config
		destCatalogueName := connectionMap[connectionID].DestinationDetail.Catalogue.Name

		var typeMapKassetteToDest map[string]string
		var destConverter integrations.TransformerHandleI
		var destSkipWithNoSchema bool
		var typeMapKassetteToSrc map[string]string
		var srcConverter sources.TransformerHandleI
		var srcSkipWithNoSchema bool

		switch destCatalogueName {
		case "Postgres":
			typeMapKassetteToDest = postgres.TypeMapKassetteToDest
			destConverter = &postgres.TransformerHandleT{}
			destSkipWithNoSchema = true
		case "PowerBI":
			typeMapKassetteToDest = powerbi.TypeMapKassetteToDest
			destConverter = &powerbi.TransformerHandleT{}
			destSkipWithNoSchema = false
		case "Anaplan":
			typeMapKassetteToDest = anaplan.TypeMapKassetteToDest
			destConverter = &anaplan.TransformerHandleT{}
			destSkipWithNoSchema = false
		}

		switch srcCatalogueName {
		case "Camunda":
			typeMapKassetteToSrc = sources.TypeMapKassetteToSrc
			srcConverter = &camunda.TransformerHandleT{}
			srcSkipWithNoSchema = false
		}

		configSubscriberLock.RUnlock()
		response := proc.transformer.Transform(destEventList, transformRule, destConfigData, destConverter, typeMapKassetteToDest, destSkipWithNoSchema, srcConfigData, srcConverter, typeMapKassetteToSrc, srcSkipWithNoSchema, transformBatchSize)
		destTransformEventList := response.Events
		logger.Info(fmt.Sprintf("Transform output size: %d", len(response.Events)))
		
		if !response.Success {
			logger.Error(fmt.Sprintf("Error in transformation for connection %v", connectionID))
			continue
		}

		destEventJSON, err := json.Marshal(destTransformEventList)
		if err != nil {
			logger.Error(fmt.Sprintf("Error marshalling transformed event %v", err))
			continue
		}

		//Need to replace UUID his with messageID from client
		configSubscriberLock.RLock()
		destination := connectionMap[connectionID].DestinationDetail.Destination
		source := connectionMap[connectionID].SourceDetail.Source
		configSubscriberLock.RUnlock()
		id := uuid.New()
		newJob := jobsdb.JobT{
			UUID:         id,
			Parameters:   []byte(fmt.Sprintf(`{"destination_id": "%v", "source_id": "%v"}`, destination.ID, source.ID)),
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
			CustomVal:    "",
			EventPayload: destEventJSON,
		}
		if misc.Contains(rawDataDestinations, newJob.CustomVal) {
			batchDestJobs = append(batchDestJobs, &newJob)
		} else {
			destJobs = append(destJobs, &newJob)
		}
	}

	logger.Info(fmt.Sprintf("Storing Router jobs: %d", len(destJobs)))
	//XX: Need to do this in a transaction
	proc.routerDB.Store(destJobs)

	logger.Info(fmt.Sprintf("Updating job status for: %d", len(batchDestJobs)))
	proc.gatewayDB.UpdateJobStatus(statusList, []string{gateway.CustomVal})
	//XX: End of transaction
}

func getTimestampFromEvent(event map[string]interface{}, field string) time.Time {
	var timestamp time.Time
	var err error
	if _, ok := event[field]; ok {
		timestampStr, typecasted := event[field].(string)
		if typecasted {
			timestamp, err = dateparse.ParseAny(timestampStr)
		}
		if !typecasted || err != nil {
			timestamp = time.Now()
		}
	} else {
		timestamp = time.Now()
	}
	return timestamp
}

func (proc *HandleT) addJobsToSessions(jobList []*jobsdb.JobT) {

	logger.Debug(fmt.Sprintf("Adding %d jobs to sessions", len(jobList)))

	proc.userPQLock.Lock()

	//List of users whose jobs need to be processed
	processUserIDs := make(map[string]bool)

	for _, job := range jobList {
		//Append to job to list. If over threshold, just process them
		eventList, ok := misc.ParseKassetteEventBatch(job.EventPayload)
		if !ok {
			//bad event
			failedPayload, _ := job.EventPayload.MarshalJSON()
			logger.Error(fmt.Sprintf("Failed to parse event: %v", failedPayload))
			continue
		}
		userID, ok := misc.GetKassetteEventUserID(eventList)
		if !ok {
			//logger.Error("Failed to get userID for job")
			//continue
			logger.Info("Failed to get userID for job, setting to default")
			userID = "default"
		}
		_, ok = proc.userJobListMap[userID]
		if !ok {
			proc.userJobListMap[userID] = make([]*jobsdb.JobT, 0)
			proc.userEventsMap[userID] = make([]interface{}, 0)
		}
		//Add the job to the userID specific lists
		proc.userJobListMap[userID] = append(proc.userJobListMap[userID], job)
		proc.userEventsMap[userID] = append(proc.userEventsMap[userID], eventList...)
		//If we have enough events from that user, we process jobs
		if len(proc.userEventsMap[userID]) > sessionThresholdEvents {
			processUserIDs[userID] = true
		}
		pqItem, ok := proc.userPQItemMap[userID]
		if !ok {
			pqItem := &pqItemT{
				userID: userID,
				lastTS: time.Now(),
				index:  -1,
			}
			proc.userPQItemMap[userID] = pqItem
			proc.userJobPQ.Add(pqItem)
		} else {
			misc.Assert(pqItem.index != -1)
			proc.userJobPQ.Update(pqItem, time.Now())
		}
	}

	if len(processUserIDs) > 0 {
		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]interface{})
		logger.Info("Post Add Processing")

		//We clear the data structure for these users
		for userID := range processUserIDs {
			userJobsToProcess[userID] = proc.userJobListMap[userID]
			userEventsToProcess[userID] = proc.userEventsMap[userID]
			delete(proc.userJobListMap, userID)
			delete(proc.userEventsMap, userID)
			proc.userJobPQ.Remove(proc.userPQItemMap[userID])
			delete(proc.userPQItemMap, userID)
		}
		logger.Info("Processing")
		proc.Print()
		//We release the block before actually processing
		proc.userPQLock.Unlock()
		proc.processUserJobs(userJobsToProcess, userEventsToProcess)
		return
	}
	proc.userPQLock.Unlock()
}

func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		detail := config.Data.(backendconfig.ConnectionDetailsT)
		connectionMap = map[int]backendconfig.ConnectionDetailT{}
		for _, conn := range detail.Connections {
			connection := conn.Connection
			connectionMap[connection.ID] = conn
		}
		configInitialized = true
		configSubscriberLock.Unlock()
	}
}

// Print the internal structure
func (proc *HandleT) Print() {

	log.Println("PriorityQueue")
	proc.userJobPQ.Print()
	log.Println("JobList")
	for k, v := range proc.userJobListMap {
		log.Println(k, ":", len(v))
	}
	log.Println("EventLength")
	for k, v := range proc.userEventsMap {
		log.Println(k, ":", len(v))
	}
	log.Println("PQItem")
	for k, v := range proc.userPQItemMap {
		log.Println(k, ":", *v)
	}
}

func (proc *HandleT) processUserJobs(userJobs map[string][]*jobsdb.JobT, userEvents map[string][]interface{}) {

	misc.Assert(len(userEvents) == len(userJobs))

	totalJobs := 0
	allJobIDs := make(map[int64]bool)
	for userID := range userJobs {
		for _, job := range userJobs[userID] {
			totalJobs++
			allJobIDs[job.JobID] = true
		}
	}

	//Create a list of list of user events which is passed to transformer
	userEventsList := make([]interface{}, 0)
	userIDList := make([]string, 0) //Order of users which are added to list
	for userID := range userEvents {
		userEventsList = append(userEventsList, userEvents[userID])
		userIDList = append(userIDList, userID)
	}
	misc.Assert(len(userEventsList) == len(userEvents))

	//Create jobs that can be processed further
	toProcessJobs, toProcessEvents := createUserTransformedJobsFromEvents(userEventsList, userIDList, userJobs)

	//Some sanity check to make sure we have all the jobs
	misc.Assert(len(toProcessJobs) == totalJobs)
	misc.Assert(len(toProcessEvents) == totalJobs)
	for _, job := range toProcessJobs {
		_, ok := allJobIDs[job.JobID]
		misc.Assert(ok)
		delete(allJobIDs, job.JobID)
	}
	misc.Assert(len(allJobIDs) == 0)

	//Process
	proc.processJobsForDest(toProcessJobs, toProcessEvents)
}

// We create sessions (of individul events) from set of input jobs  from a user
// Those sesssion events are transformed and we have a transformed set of
// events that must be processed further via destination specific transformations
// (in processJobsForDest). This function creates jobs from eventList
func createUserTransformedJobsFromEvents(transformUserEventList []interface{},
	userIDList []string, userJobs map[string][]*jobsdb.JobT) ([]*jobsdb.JobT, [][]interface{}) {

	transJobList := make([]*jobsdb.JobT, 0)
	transEventList := make([][]interface{}, 0)
	misc.Assert(len(transformUserEventList) == len(userIDList))
	for idx, userID := range userIDList {
		userEvents := transformUserEventList[idx]
		userEventsList, ok := userEvents.([]interface{})
		misc.Assert(ok)
		for idx, job := range userJobs[userID] {
			//We put all the transformed event on the first job
			//and empty out the remaining payloads
			transJobList = append(transJobList, job)
			if idx == 0 {
				transEventList = append(transEventList, userEventsList)
			} else {
				transEventList = append(transEventList, nil)
			}
		}
	}
	return transJobList, transEventList
}

func (proc *HandleT) createSessions() {

	for {
		proc.userPQLock.Lock()
		//Now jobs
		if proc.userJobPQ.Len() == 0 {
			proc.userPQLock.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		//Enough time hasn't transpired since last
		oldestItem := proc.userJobPQ.Top()
		if time.Since(oldestItem.lastTS) < time.Duration(sessionThresholdInS) {
			proc.userPQLock.Unlock()
			sleepTime := time.Duration(sessionThresholdInS) - time.Since(oldestItem.lastTS)
			logger.Debug(fmt.Sprint("Sleeping", sleepTime.String()))
			time.Sleep(sleepTime)
			continue
		}

		userJobsToProcess := make(map[string][]*jobsdb.JobT)
		userEventsToProcess := make(map[string][]interface{})
		//Find all jobs that need to be processed
		for {
			if proc.userJobPQ.Len() == 0 {
				break
			}
			oldestItem := proc.userJobPQ.Top()
			if time.Since(oldestItem.lastTS) > time.Duration(sessionThresholdInS) {
				userID := oldestItem.userID
				pqItem, ok := proc.userPQItemMap[userID]
				misc.Assert(ok && pqItem == oldestItem)
				userJobsToProcess[userID] = proc.userJobListMap[userID]
				userEventsToProcess[userID] = proc.userEventsMap[userID]
				//Clear from the map
				delete(proc.userJobListMap, userID)
				delete(proc.userEventsMap, userID)
				proc.userJobPQ.Remove(proc.userPQItemMap[userID])
				delete(proc.userPQItemMap, userID)
				continue
			}
			break
		}
		proc.userPQLock.Unlock()
		if len(userJobsToProcess) > 0 {
			proc.processUserJobs(userJobsToProcess, userEventsToProcess)
		} else {
			logger.Debug("No jobs to process")
		}
	}
}
