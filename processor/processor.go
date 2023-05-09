package processor

import (
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"sort"
	"sync"
	"time"
)

type HandleT struct {
	gatewayDB     *jobsdb.HandleT
	routerDB      *jobsdb.HandleT
	batchRouterDB *jobsdb.HandleT
	transformer   *transformerHandleT
	//statsJobs          *misc.PerfStats
	//statsDBR           *misc.PerfStats
	//statGatewayDBR     *stats.RudderStats
	//statsDBW           *misc.PerfStats
	//statGatewayDBW     *stats.RudderStats
	//statRouterDBW      *stats.RudderStats
	//statBatchRouterDBW *stats.RudderStats
	//statActiveUsers    *stats.RudderStats
	userJobListMap map[string][]*jobsdb.JobT
	userEventsMap  map[string][]interface{}
	userPQItemMap  map[string]*pqItemT
	userJobPQ      pqT
	userPQLock     sync.Mutex
}

func (proc *HandleT) mainLoop() {

	for {

		toQuery := 10
		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := proc.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery)

		unprocessedList := proc.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			proc.statsDBR.End(0)
			time.Sleep(loopSleep)
			continue
		}

		combinedList := append(unprocessedList, retryList...)
		proc.statsDBR.End(len(combinedList))
		proc.statGatewayDBR.Count(len(combinedList))

		proc.statsDBR.Print()

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
					AttemptNum:    1,
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
			proc.processJobsForDest(combinedList, nil)
		}

	}
}
