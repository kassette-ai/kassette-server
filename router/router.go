package router

import (
	"fmt"
	"hash/fnv"
	"kassette.ai/kassette-server/integrations"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/misc"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type HandleT struct {
	requestQ              chan *jobsdb.JobT
	responseQ             chan jobResponseT
	jobsDB                *jobsdb.HandleT
	netHandle             *NetHandleT
	destID                string
	workers               []*workerT
	successCount          uint64
	failCount             uint64
	isEnabled             bool
	toClearFailJobIDMutex sync.Mutex
	toClearFailJobIDMap   map[int][]string
}

type NetHandleT struct {
	httpClient *http.Client
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel          chan *jobsdb.JobT // the worker job channel
	workerID         int               // identifies the worker
	failedJobs       int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime        time.Duration     //the sleep duration for every job of the worker
	failedJobIDMap   map[string]int64  //user to failed jobId
	failedJobIDMutex sync.RWMutex      //lock to protect structure above
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser int
	maxFailedCountForJob                                                           int
	readSleep, minSleep, maxSleep, maxStatusUpdateWait                             time.Duration
	randomWorkerAssign, useTestSink, keepOrderOnFailure                            bool
	testSinkURL                                                                    string
)

func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, destID string) {

	rt.jobsDB = jobsDB
	rt.destID = destID

	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.toClearFailJobIDMap = make(map[int][]string)
	rt.isEnabled = true
	rt.netHandle = &NetHandleT{}
	rt.netHandle.Setup(destID)

	rt.initWorkers()
	go rt.statusInsertLoop()
	go rt.generatorLoop()
}

func (rt *HandleT) statusInsertLoop() {

	var responseList []jobResponseT
	//Wait for the responses from statusQ
	lastUpdate := time.Now()

	for {

		select {
		case jobStatus := <-rt.responseQ:
			log.Printf("%v Router :: Got back status error %v and state %v for job %v", rt.destID, jobStatus.status.ErrorCode,
				jobStatus.status.JobState, jobStatus.status.JobID)
			responseList = append(responseList, jobStatus)

		case <-time.After(maxStatusUpdateWait):

			//Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			//but approx is good enough at the cost of reduced computation.
		}

		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {

			var statusList []*jobsdb.JobStatusT
			for _, resp := range responseList {
				statusList = append(statusList, resp.status)
			}

			if len(statusList) > 0 {
				log.Printf("%v Router :: flushing batch of %v status", rt.destID, updateStatusBatchSize)

				sort.Slice(statusList, func(i, j int) bool {
					return statusList[i].JobID < statusList[j].JobID
				})
				//Update the status
				rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
			}

			//#JobOrder (see other #JobOrder comment)
			for _, resp := range responseList {
				status := resp.status.JobState
				userID := resp.userID
				worker := resp.worker
				if status == jobsdb.SucceededState || status == jobsdb.AbortedState {
					worker.failedJobIDMutex.RLock()
					lastJobID, ok := worker.failedJobIDMap[userID]
					worker.failedJobIDMutex.RUnlock()
					if ok && lastJobID == resp.status.JobID {
						rt.toClearFailJobIDMutex.Lock()
						log.Printf("%v Router :: clearing failedJobIDMap for userID: %v", rt.destID, userID)
						_, ok := rt.toClearFailJobIDMap[worker.workerID]
						if !ok {
							rt.toClearFailJobIDMap[worker.workerID] = make([]string, 0)
						}
						rt.toClearFailJobIDMap[worker.workerID] = append(rt.toClearFailJobIDMap[worker.workerID], userID)
						rt.toClearFailJobIDMutex.Unlock()
					}
				}
			}
			//End #JobOrder
			responseList = nil
			lastUpdate = time.Now()

		}
	}

}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		log.Println("Worker Started", i)
		var worker *workerT
		worker = &workerT{
			channel:        make(chan *jobsdb.JobT, noOfJobsPerChannel),
			failedJobIDMap: make(map[string]int64),
			workerID:       i,
			failedJobs:     0,
			sleepTime:      minSleep}
		rt.workers[i] = worker
		go rt.workerProcess(worker)
	}
}

func (rt *HandleT) workerProcess(worker *workerT) {

	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, respBody string

		log.Println("Router :: trying to send payload to GA", respBody)

		postInfo := integrations.GetPostInfo(job.EventPayload)
		userID := postInfo.UserID
		misc.Assert(userID != "")

		//If sink is not enabled mark all jobs as waiting
		if !rt.isEnabled {

			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				JobState:      jobsdb.WaitingState,
				ErrorResponse: []byte(`{"reason":"Router Disabled"}`), // check
			}
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
			continue
		}

		//If there is a failed jobID from this user, we cannot pass future jobs
		worker.failedJobIDMutex.RLock()
		previousFailedJobID, isPrevFailedUser := worker.failedJobIDMap[userID]
		worker.failedJobIDMutex.RUnlock()

		if isPrevFailedUser && previousFailedJobID < job.JobID {
			log.Printf("%v Router :: skipping processing job for userID: %v since prev failed job exists, prev id %v, current id %v\n", rt.destID, userID, previousFailedJobID, job.JobID)
			resp := fmt.Sprintf(`{"blocking_id":"%v", "user_id":"%s"}`, previousFailedJobID, userID)
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     respStatus,
				JobState:      jobsdb.WaitingState,
				ErrorResponse: []byte(resp), // check
			}
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
			continue
		}

		if isPrevFailedUser {
			misc.Assert(previousFailedJobID == job.JobID)
		}

		//We can execute thoe job
		for attempts = 0; attempts < ser; attempts++ {
			log.Println("%v Router :: trying to send payload %v of %v", rt.destID, attempts, ser)

			respStatusCode, respStatus, respBody = rt.netHandle.sendPost(job.EventPayload)

			if useTestSink {
				//Internal test. No reason to sleep
				break
			}
			if respStatusCode != http.StatusOK {
				//400 series error are client errors. Can't continue
				if respStatusCode >= http.StatusBadRequest && respStatusCode <= http.StatusUnavailableForLegalReasons {
					break
				}
				//Wait before the next retry
				worker.sleepTime = 2*worker.sleepTime + 1 //+1 handles 0 sleepTime
				if worker.sleepTime > maxSleep {
					worker.sleepTime = maxSleep
				}
				log.Printf("%v Router :: worker %v sleeping for  %v ",
					rt.destID, worker.workerID, worker.sleepTime)
				time.Sleep(worker.sleepTime * time.Second)
				continue
			} else {
				atomic.AddUint64(&rt.successCount, 1)
				//Divide the sleep
				worker.sleepTime = worker.sleepTime / 2
				if worker.sleepTime < minSleep {
					worker.sleepTime = minSleep
				}
				log.Printf("%v Router :: sleep for worker %v decreased to %v",
					rt.destID, worker.workerID, worker.sleepTime)
				break
			}
		}

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ErrorCode:     respStatus,
			ErrorResponse: []byte(`{}`),
		}

		if respStatusCode == http.StatusOK {
			//#JobOrder (see other #JobOrder comment)

			status.AttemptNum = job.LastJobStatus.AttemptNum
			status.JobState = jobsdb.SucceededState
			log.Printf("%v Router :: sending success status to response", rt.destID)
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		} else {
			// the job failed
			log.Printf("%v Router :: Job failed to send, analyzing...", rt.destID)
			worker.failedJobs++
			atomic.AddUint64(&rt.failCount, 1)

			//#JobOrder (see other #JobOrder comment)
			if !isPrevFailedUser && keepOrderOnFailure {
				log.Printf("%v Router :: userId %v failed for the first time adding to map", rt.destID, userID)
				worker.failedJobIDMutex.Lock()
				worker.failedJobIDMap[userID] = job.JobID
				worker.failedJobIDMutex.Unlock()
			}

			switch {
			case len(worker.failedJobIDMap) > 5:
				//Lot of jobs are failing in this worker. Likely the sink is down
				//We still mark the job failed but don't increment the AttemptNum
				//This is a heuristic. Will fix it with Sayan's idea
				status.JobState = jobsdb.FailedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				log.Printf("%v Router :: Marking job as failed and not incrementing the AttemptNum since jobs from more than 5 users are failing for destination", rt.destID)
				break
			case status.AttemptNum >= maxFailedCountForJob:
				//The job has failed enough number of times so mark it aborted
				//The reason for doing this is to filter out jobs with bad payload
				//which can never succeed.
				//However, there is a risk that if sink is down, a set of jobs can
				//reach maxCountFailure. In practice though, when sink goes down
				//lot of jobs will fail and all will get retried in batch with
				//doubling sleep in between. That case will be handled in case above
				log.Printf("%v Router :: Aborting the job and deleting from user map", rt.destID)
				status.JobState = jobsdb.AbortedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			default:
				status.JobState = jobsdb.FailedState
				status.AttemptNum = job.LastJobStatus.AttemptNum + 1
				log.Printf("%v Router :: Marking job as failed and incrementing the AttemptNum", rt.destID)
				break
			}
			log.Printf("%v Router :: sending failed/aborted state as response", rt.destID)
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		}

	}
}

//#JobOrder (see other #JobOrder comment)
//If a job fails (say with given failed_job_id), we need to fail other jobs from that user till
//the failed_job_id succeeds. We achieve this by keeping the failed_job_id in a failedJobIDMap
//structure (mapping userID to failed_job_id). All subsequent jobs (guaranteed to be job_id >= failed_job_id)
//are put in WaitingState in worker loop till the failed_job_id succeeds.
//However, the step of removing failed_job_id from the failedJobIDMap structure is QUITE TRICKY.
//To understand that, need to understand the complete lifecycle of a job.
//The job goes through the following data-structures in order
//   i>   generatorLoop Buffer (read from DB)
//   ii>  requestQ
//   iii> Worker Process
//   iv>  responseQ
//   v>   statusInsertLoop Buffer (enough jobs are buffered before updating status)
// Now, when the failed_job_id eventually suceeds in the Worker Process (iii above),
// there may be pending jobs in all the other data-structures. For example, there
//may be jobs in responseQ(iv) and statusInsertLoop(v) buffer - all those jobs will
//be in Waiting state. Similarly, there may be other jobs in requestQ and generatorLoop
//buffer.
//If the failed_job_id succeeds and we remove the filter gate, then all the jobs in requestQ
//will pass through before the jobs in responseQ/insertStatus buffer. That will violate the
//ordering of job.
//We fix this by removing an entry from the failedJobIDMap structure only when we are guaranteed
//that all the other structures are empty. We do the following to ahieve this
// A. In generatorLoop, we do not let any job pass through except failed_job_id. That ensures requestQ is empty
// B. We wait for the failed_job_id status (when succeeded) to be sync'd to disk. This along with A ensures
//    that responseQ and statusInsertLoop Buffer are empty for that userID.
// C. Finally, we want for generatorLoop buffer to be fully processed.

func (rt *HandleT) generatorLoop() {

	log.Println("Generator started")

	for {
		if !rt.isEnabled {
			time.Sleep(1000)
			continue
		}

		//#JobOrder (See comment marked #JobOrder
		rt.toClearFailJobIDMutex.Lock()
		for idx := range rt.toClearFailJobIDMap {
			wrk := rt.workers[idx]
			wrk.failedJobIDMutex.Lock()
			for _, userID := range rt.toClearFailJobIDMap[idx] {
				delete(wrk.failedJobIDMap, userID)
			}
			wrk.failedJobIDMutex.Unlock()
		}
		rt.toClearFailJobIDMap = make(map[int][]string)
		rt.toClearFailJobIDMutex.Unlock()
		//End of #JobOrder

		toQuery := jobQueryBatchSize
		retryList := rt.jobsDB.GetToRetry([]string{rt.destID}, toQuery)
		toQuery -= len(retryList)
		waitList := rt.jobsDB.GetWaiting([]string{rt.destID}, toQuery) //Jobs send to waiting state
		toQuery -= len(waitList)
		unprocessedList := rt.jobsDB.GetUnprocessed([]string{rt.destID}, toQuery)
		if len(waitList)+len(unprocessedList)+len(retryList) == 0 {
			time.Sleep(readSleep)
			continue
		}

		combinedList := append(waitList, append(unprocessedList, retryList...)...)

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		if len(combinedList) > 0 {
			log.Printf("%v Router :: router is enabled", rt.destID)
			log.Printf("%v Router ===== len to be processed==== : %v", rt.destID, len(combinedList))
		}

		//List of jobs wich can be processed mapped per channel
		type workerJobT struct {
			worker *workerT
			job    *jobsdb.JobT
		}

		var statusList []*jobsdb.JobStatusT
		var toProcess []workerJobT

		//Identify jobs which can be processed
		for _, job := range combinedList {
			w := rt.findWorker(job)
			if w != nil {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.ExecutingState,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`), // check
				}
				statusList = append(statusList, &status)
				toProcess = append(toProcess, workerJobT{worker: w, job: job})
			}
		}

		//Mark the jobs as executing
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})

		//Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.channel <- wrkJob.job
		}

	}
}

func (rt *HandleT) findWorker(job *jobsdb.JobT) *workerT {

	postInfo := integrations.GetPostInfo(job.EventPayload)

	var index int
	if randomWorkerAssign {
		index = rand.Intn(noOfWorkers)
	} else {
		index = int(math.Abs(float64(getHash(postInfo.UserID) % noOfWorkers)))
	}

	worker := rt.workers[index]
	misc.Assert(worker != nil)

	//#JobOrder (see other #JobOrder comment)
	worker.failedJobIDMutex.RLock()
	defer worker.failedJobIDMutex.RUnlock()
	blockJobID, found := worker.failedJobIDMap[postInfo.UserID]
	if !found {
		return worker
	}
	//This job can only be higher than blocking
	//We only let the blocking job pass
	misc.Assert(job.JobID >= blockJobID)
	if job.JobID == blockJobID {
		return worker
	}
	return nil
	//#EndJobOrder
}

func getHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (rt *HandleT) Enable() {
	rt.isEnabled = true
}

// Disable disables a router:)
func (rt *HandleT) Disable() {
	rt.isEnabled = false
}