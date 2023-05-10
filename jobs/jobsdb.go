package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/spf13/viper"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type JobT struct {
	UUID          uuid.UUID       `json:"UUID"`
	JobID         int64           `json:"JobID"`
	UserID        string          `json:"UserID"`
	CreatedAt     time.Time       `json:"CreatedAt"`
	ExpireAt      time.Time       `json:"ExpireAt"`
	CustomVal     string          `json:"CustomVal"`
	EventCount    int             `json:"EventCount"`
	EventPayload  json.RawMessage `json:"EventPayload"`
	PayloadSize   int64           `json:"PayloadSize"`
	LastJobStatus JobStatusT      `json:"LastJobStatus"`
	Parameters    json.RawMessage `json:"Parameters"`
	WorkspaceId   string          `json:"WorkspaceId"`
}

type JobStatusT struct {
	JobID         int64           `json:"JobID"`
	JobState      string          `json:"JobState"` // ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
	Parameters    json.RawMessage `json:"Parameters"`
	JobParameters json.RawMessage `json:"-"`
	WorkspaceId   string          `json:"WorkspaceId"`
}

// constants for JobStatusT JobState
const (
	SucceededState    = "succeeded"
	FailedState       = "failed"
	ExecutingState    = "executing"
	AbortedState      = "aborted"
	WaitingState      = "waiting"
	WaitingRetryState = "waiting_retry"
	InternalState     = "NP"
)

// OwnerType for this jobsdb instance
type OwnerType string

const (
	// Read : Only Reader of this jobsdb instance
	Read OwnerType = "READ"
	// Write : Only Writer of this jobsdb instance
	Write OwnerType = "WRITE"
	// ReadWrite : Reader and Writer of this jobsdb instance
	ReadWrite OwnerType = ""
)

type HandleT struct {
	dbHandle         *sql.DB
	ownerType        OwnerType
	tablePrefix      string
	datasetList      []dataSetT
	datasetRangeList []dataSetRangeT

	MinDSRetentionPeriod time.Duration
	MaxDSRetentionPeriod time.Duration

	newDSCreationTime             time.Time
	isStatNewDSPeriodInitialized  bool
	dsDropTime                    time.Time
	isStatDropDSPeriodInitialized bool

	writeCapacity      chan struct{}
	readCapacity       chan struct{}
	enableWriterQueue  bool
	enableReaderQueue  bool
	clearAll           bool
	dsLimit            *int
	maxReaders         int
	maxWriters         int
	maxOpenConnections int
	analyzeThreshold   int
	MaxDSSize          *int
	backgroundCancel   context.CancelFunc

	maxBackupRetryTime time.Duration

	// skipSetupDBSetup is useful for testing as we mock the database client
	// TODO: Remove this flag once we have test setup that uses real database
	skipSetupDBSetup bool

	// TriggerAddNewDS, TriggerMigrateDS is useful for triggering addNewDS to run from tests.
	// TODO: Ideally we should refactor the code to not use this override.
	TriggerAddNewDS  func() <-chan time.Time
	TriggerMigrateDS func() <-chan time.Time
	migrateDSTimeout time.Duration

	TriggerRefreshDS func() <-chan time.Time
	refreshDSTimeout time.Duration

	lifecycle struct {
		mu      sync.Mutex
		started bool
	}
}

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
}

type dataSetRangeT struct {
	minJobID  int64
	maxJobID  int64
	startTime int64
	endTime   int64
	ds        dataSetT
}

/*
Setup is used to initialize the HandleT structure.
clearAll = True means it will remove all existing tables
tablePrefix must be unique and is used to separate
multiple users of JobsDB
dsRetentionPeriod = A DS is not deleted if it has some activity
in the retention time
*/
func (jd *HandleT) Setup(clearAll bool, tablePrefix string, retentionPeriod time.Duration, toBackup bool) {

	var err error
	psqlInfo := GetConnectionString()

	jd.tablePrefix = tablePrefix
	//jd.dsRetentionPeriod = retentionPeriod
	//jd.toBackup = toBackup
	//jd.dsEmptyResultCache = map[dataSetT]map[string]map[string]bool{}

	jd.dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Failed to open DB connection", err)
	}

	log.Println("Connected to DB")
	err = jd.dbHandle.Ping()

	jd.setupEnumTypes(psqlInfo)

	jd.getDSList(true)

	//If no DS present, add one
	// this will be configurable in the future
	if len(jd.datasetList) == 0 {
		jd.addNewDS(true, dataSetT{})
	}

}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		viper.Get("POSTGRES_HOST"),
		viper.Get("POSTGRES_PORT"),
		viper.Get("POSTGRES_USER"),
		viper.Get("POSTGRES_PASSWORD"),
		viper.Get("POSTGRES_DB"))
}

func (jd *HandleT) setupEnumTypes(psqlInfo string) {

	dbHandle, err := sql.Open("postgres", psqlInfo)
	defer dbHandle.Close()

	sqlStatement := `DO $$ BEGIN
                                CREATE TYPE job_state_type
                                     AS ENUM(
                                              'waiting',
                                              'executing',
                                              'succeeded',
                                              'waiting_retry',
                                              'failed',
                                              'aborted');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		log.Fatal("Failed to setup enum types", err)
	}
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
Caller must have the dsListLock readlocked
*/
func (jd *HandleT) getDSList(refreshFromDB bool) []dataSetT {

	if !refreshFromDB {
		return jd.datasetList
	}

	//At this point we MUST have write-locked dsListLock
	//since we are modiying the list

	//Reset the global list
	jd.datasetList = nil

	//Read the table names from PG
	tableNames := jd.getAllTableNames()

	//Tables are of form jobs_ and job_status_. Iterate
	//through them and sort them to produce and
	//ordered list of datasets

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	dnumList := []string{}

	for _, t := range tableNames {
		if strings.HasPrefix(t, jd.tablePrefix+"_jobs_") {
			dnum := t[len(jd.tablePrefix+"_jobs_"):]
			jobNameMap[dnum] = t
			dnumList = append(dnumList, dnum)
			continue
		}
		if strings.HasPrefix(t, jd.tablePrefix+"_job_status_") {
			dnum := t[len(jd.tablePrefix+"_job_status_"):]
			jobStatusNameMap[dnum] = t
			continue
		}
	}

	//Create the structure
	for _, dnum := range dnumList {
		jobName, _ := jobNameMap[dnum]

		jobStatusName, _ := jobStatusNameMap[dnum]
		jd.datasetList = append(jd.datasetList,
			dataSetT{JobTable: jobName,
				JobStatusTable: jobStatusName, Index: dnum})
	}

	return jd.datasetList
}

// Function to get all table names form Postgres
func (jd *HandleT) getAllTableNames() []string {
	//Read the table names from PG
	stmt, _ := jd.dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)

	defer stmt.Close()

	rows, _ := stmt.Query()
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		_ = rows.Scan(&tbName)

		tableNames = append(tableNames, tbName)
	}

	return tableNames
}

func (jd *HandleT) addNewDS(appendLast bool, insertBeforeDS dataSetT) dataSetT {

	newDSIdx := "1"

	var newDS dataSetT
	newDS.JobTable, newDS.JobStatusTable = jd.createTableNames(newDSIdx)
	newDS.Index = newDSIdx

	//Create the jobs and job_status tables
	sqlStatement := fmt.Sprintf(`CREATE TABLE %s (
                                      job_id BIGSERIAL PRIMARY KEY,
                                      uuid UUID NOT NULL,
									  parameters JSONB NOT NULL,
                                      custom_val VARCHAR(64) NOT NULL,
                                      event_payload JSONB NOT NULL,
                                      created_at TIMESTAMP NOT NULL,
                                      expire_at TIMESTAMP NOT NULL);`, newDS.JobTable)

	_, _ = jd.dbHandle.Exec(sqlStatement)

	sqlStatement = fmt.Sprintf(`CREATE TABLE %s (
                                     id BIGSERIAL PRIMARY KEY,
                                     job_id INT REFERENCES %s(job_id),
                                     job_state job_state_type,
                                     attempt SMALLINT,
                                     exec_time TIMESTAMP,
                                     retry_time TIMESTAMP,
                                     error_code VARCHAR(32),
                                     error_response JSONB);`, newDS.JobStatusTable, newDS.JobTable)

	//This is the migration case. We don't yet update the in-memory list till
	//we finish the migration
	_, _ = jd.dbHandle.Exec(sqlStatement)

	return newDS
}

func (jd *HandleT) createTableNames(dsIdx string) (string, string) {
	jobTable := fmt.Sprintf("%s_jobs_%s", jd.tablePrefix, dsIdx)
	jobStatusTable := fmt.Sprintf("%s_job_status_%s", jd.tablePrefix, dsIdx)
	return jobTable, jobStatusTable
}

func (jd *HandleT) storeJobDS(ds dataSetT, job *JobT) (errorMessage string) {

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (uuid, custom_val, parameters, event_payload, created_at, expire_at)
                                       VALUES ($1, $2, $3, $4, $5, $6) RETURNING job_id`, ds.JobTable)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)

	defer stmt.Close()

	_, err = stmt.Exec(job.UUID, job.CustomVal, string(job.Parameters), string(job.EventPayload),
		job.CreatedAt, job.ExpireAt)
	if err == nil {
		return
	}
	pqErr := err.(*pq.Error)
	log.Fatal("Failed to store job", pqErr)
	return
}

func (jd *HandleT) updateJobStatusDS(ds dataSetT, statusList []*JobStatusT, customValFilters []string) (err error) {

	if len(statusList) == 0 {
		return nil
	}

	txn, err := jd.dbHandle.Begin()

	stmt, err := txn.Prepare(pq.CopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
		"retry_time", "error_code", "error_response"))

	defer stmt.Close()
	for _, status := range statusList {
		//  Handle the case when google analytics returns gif in response
		if !utf8.ValidString(string(status.ErrorResponse)) {
			status.ErrorResponse, _ = json.Marshal("{}")
		}
		_, err = stmt.Exec(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
			status.RetryTime, status.ErrorCode, string(status.ErrorResponse))

	}
	_, err = stmt.Exec()

	err = txn.Commit()

	//Get all the states and clear from empty cache
	stateFiltersMap := map[string]bool{}
	for _, st := range statusList {
		stateFiltersMap[st.JobState] = true
	}
	stateFilters := make([]string, 0, len(stateFiltersMap))
	for k := range stateFiltersMap {
		stateFilters = append(stateFilters, k)
	}

	return nil
}

func (jd *HandleT) GetToRetry(customValFilters []string, count int, sourceIDFilters ...string) []*JobT {
	return jd.GetProcessed([]string{FailedState}, customValFilters, count, sourceIDFilters...)
}

func (jd *HandleT) getProcessedJobsDS(ds dataSetT, getAll bool, stateFilters []string,
	customValFilters []string, limitCount int, sourceIDFilters ...string) ([]*JobT, error) {

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + jd.constructQuery("job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {

		customValQuery = " AND " +
			jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(sourceIDFilters) > 0 {

		sourceQuery += " AND " + jd.constructJSONQuery(fmt.Sprintf("%s.parameters", ds.JobTable), "source_id",
			sourceIDFilters, "OR")
	} else {
		sourceQuery = ""
	}

	if limitCount > 0 {

		limitQuery = fmt.Sprintf(" LIMIT %d ", limitCount)
	} else {
		limitQuery = ""
	}

	var rows *sql.Rows
	if getAll {
		sqlStatement := fmt.Sprintf(`SELECT
                                  %[1]s.job_id, %[1]s.uuid, %[1]s.parameters,  %[1]s.custom_val, %[1]s.event_payload,
                                  %[1]s.created_at, %[1]s.expire_at,
                                  job_latest_state.job_state, job_latest_state.attempt,
                                  job_latest_state.exec_time, job_latest_state.retry_time,
                                  job_latest_state.error_code, job_latest_state.error_response
                                 FROM
                                  %[1]s,
                                  (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                    error_code, error_response FROM %[2]s WHERE id IN
                                    (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                  AS job_latest_state
                                   WHERE %[1]s.job_id=job_latest_state.job_id`,
			ds.JobTable, ds.JobStatusTable, stateQuery)
		var err error

		rows, err = jd.dbHandle.Query(sqlStatement)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

	} else {
		sqlStatement := fmt.Sprintf(`SELECT
                                               %[1]s.job_id, %[1]s.uuid,  %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
                                               %[1]s.created_at, %[1]s.expire_at,
                                               job_latest_state.job_state, job_latest_state.attempt,
                                               job_latest_state.exec_time, job_latest_state.retry_time,
                                               job_latest_state.error_code, job_latest_state.error_response
                                            FROM
                                               %[1]s,
                                               (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                                 error_code, error_response FROM %[2]s WHERE id IN
                                                   (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                               AS job_latest_state
                                            WHERE %[1]s.job_id=job_latest_state.job_id
                                             %[4]s %[5]s
                                             AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
			ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
		// fmt.Println(sqlStatement)

		stmt, err := jd.dbHandle.Prepare(sqlStatement)

		if err != nil {
			return nil, err
		}

		defer stmt.Close()
		rows, err = stmt.Query(time.Now())
		defer rows.Close()
	}

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
			&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
			&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
			&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse)

		if err != nil {
			return nil, err
		}

		jobList = append(jobList, &job)
	}

	return jobList, nil
}

/*
GetProcessed returns events of a given state. This does not update any state itself and
relises on the caller to update it. That means that successive calls to GetProcessed("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the the processors
*/
func (jd *HandleT) GetProcessed(stateFilter []string, customValFilters []string, count int, sourceIDFilters ...string) []*JobT {

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)

	if count == 0 {
		return outJobs
	}

	for _, ds := range dsList {
		jobs, err := jd.getProcessedJobsDS(ds, false, stateFilter, customValFilters, count, sourceIDFilters...)
		if err != nil {
			log.Printf("Error getting processed jobs from ds: %s", ds.JobTable)
			continue
		}

		outJobs = append(outJobs, jobs...)
		count -= len(jobs)

		if count == 0 {
			break
		}
	}

	return outJobs
}

func (jd *HandleT) GetUnprocessed(customValFilters []string, count int, sourceIDFilters ...string) []*JobT {

	//The order of lock is very important. The mainCheckLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	//jd.dsMigrationLock.RLock()
	//jd.dsListLock.RLock()
	//defer jd.dsMigrationLock.RUnlock()
	//defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)

	if count == 0 {
		return outJobs
	}
	for _, ds := range dsList {

		jobs, err := jd.getUnprocessedJobsDS(ds, customValFilters, true, count)
		if err != nil {
			log.Fatal("Failed to get unprocessed jobs", err)
		}

		outJobs = append(outJobs, jobs...)
		count -= len(jobs)

		if count == 0 {
			break
		}
	}

	//Release lock
	return outJobs
}

/*
Store call is used to create new Jobs
*/
func (jd *HandleT) Store(jobList []*JobT) map[uuid.UUID]string {

	dsList := jd.getDSList(true)
	return jd.storeJobsDS(dsList[len(dsList)-1], false, true, jobList)
}

func (jd *HandleT) storeJobsDS(ds dataSetT, copyID bool, retryEach bool, jobList []*JobT) (errorMessagesMap map[uuid.UUID]string) {

	var stmt *sql.Stmt
	var err error

	//Using transactions for bulk copying
	txn, err := jd.dbHandle.Begin()

	errorMessagesMap = make(map[uuid.UUID]string)

	if copyID {
		stmt, err = txn.Prepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "parameters", "custom_val",
			"event_payload", "created_at", "expire_at"))

	} else {
		stmt, err = txn.Prepare(pq.CopyIn(ds.JobTable, "uuid", "parameters", "custom_val", "event_payload",
			"created_at", "expire_at"))
	}

	defer stmt.Close()
	for _, job := range jobList {
		if retryEach {
			errorMessagesMap[job.UUID] = ""
		}
		if copyID {
			_, err = stmt.Exec(job.JobID, job.UUID, job.Parameters, job.CustomVal,
				string(job.EventPayload), job.CreatedAt, job.ExpireAt)
		} else {
			_, err = stmt.Exec(job.UUID, job.Parameters, job.CustomVal, string(job.EventPayload),
				job.CreatedAt, job.ExpireAt)
		}

	}
	_, err = stmt.Exec()
	if err != nil && retryEach {
		txn.Rollback() // rollback started txn, to prevent dangling db connection
		for _, job := range jobList {
			errorMessage := jd.storeJobDS(ds, job)
			errorMessagesMap[job.UUID] = errorMessage
		}
	} else {

		err = txn.Commit()

	}

	return
}

func (jd *HandleT) mapDSToLevel(ds dataSetT) (int, []int) {
	indexStr := strings.Split(ds.Index, "_")
	if len(indexStr) == 1 {
		indexLevel0, _ := strconv.Atoi(indexStr[0])

		return 1, []int{indexLevel0}
	}

	indexLevel0, _ := strconv.Atoi(indexStr[0])

	indexLevel1, _ := strconv.Atoi(indexStr[1])

	return 2, []int{indexLevel0, indexLevel1}
}

func (jd *HandleT) getUnprocessedJobsDS(ds dataSetT, customValFilters []string,
	order bool, count int, sourceIDFilters ...string) ([]*JobT, error) {

	var rows *sql.Rows

	var sqlStatement = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.parameters, %[1]s.custom_val,
                                               %[1]s.event_payload, %[1]s.created_at,
                                               %[1]s.expire_at
                                             FROM %[1]s WHERE %[1]s.job_id NOT IN (SELECT DISTINCT(%[2]s.job_id)
                                             FROM %[2]s)`, ds.JobTable, ds.JobStatusTable)

	if len(customValFilters) > 0 {
		sqlStatement += " AND " + jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
			customValFilters, "OR")
	}

	if len(sourceIDFilters) > 0 {
		sqlStatement += " AND " + jd.constructJSONQuery(fmt.Sprintf("%s.parameters", ds.JobTable), "source_id",
			sourceIDFilters, "OR")
	}

	if order {
		sqlStatement += fmt.Sprintf(" ORDER BY %s.job_id", ds.JobTable)
	}
	if count > 0 {
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
	}

	rows, _ = jd.dbHandle.Query(sqlStatement)

	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
		if err != nil {
			return nil, err
		}
		jobList = append(jobList, &job)
	}

	if len(jobList) == 0 {
		//jd.markClearEmptyResult(ds, []string{"NP"}, customValFilters, true)
	}

	return jobList, nil
}

func (jd *HandleT) constructQuery(paramKey string, paramList []string, queryType string) string {

	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	return "(" + strings.Join(queryList, " "+queryType+" ") + ")"
}

func (jd *HandleT) constructJSONQuery(paramKey string, jsonKey string, paramList []string, queryType string) string {

	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"@>'{"+fmt.Sprintf(`"%s"`, jsonKey)+":"+fmt.Sprintf(`"%s"`, p)+"}')")

	}
	return "(" + strings.Join(queryList, " "+queryType+" ") + ")"
}
