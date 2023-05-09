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
	"sync"
	"time"
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
	//jd.setupJournal()
	//jd.recoverFromJournal()

	//Refresh in memory list. We don't take lock
	//here because this is called before anything
	//else
	//jd.getDSList(true)
	//jd.getDSRangeList(true)

	////If no DS present, add one
	//if len(jd.datasetList) == 0 {
	//	jd.addNewDS(true, dataSetT{})
	//}

	//if jd.toBackup {
	//	jd.jobsFileUploader, err = fileuploader.NewFileUploader(&fileuploader.SettingsT{
	//		Provider:       "s3",
	//		AmazonS3Bucket: config.GetEnv("JOBS_BACKUP_BUCKET", ""),
	//	})
	//	jd.assertError(err)
	//	jd.jobStatusFileUploader, err = fileuploader.NewFileUploader(&fileuploader.SettingsT{
	//		Provider:       "s3",
	//		AmazonS3Bucket: config.GetEnv("JOB_STATUS_BACKUP_BUCKET", ""),
	//	})
	//	jd.assertError(err)
	//	go jd.backupDSLoop()
	//}
	//go jd.mainCheckLoop()
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		viper.Get("POSTGRES_USER"),
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

func (jd *HandleT) storeJobDS(ds dataSetT, job *JobT) (errorMessage string) {

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (uuid, custom_val, parameters, event_payload, created_at, expire_at) VALUES ($1, $2, $3, $4, $5, $6) RETURNING job_id`, ds.JobTable)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)

	defer stmt.Close()

	_, err = stmt.Exec(job.UUID, job.CustomVal, string(job.Parameters), string(job.EventPayload),
		job.CreatedAt, job.ExpireAt)
	if err == nil {
		return
	}
	pqErr := err.(*pq.Error)

	if pqErr.Fatal() {
		log.Fatal("Fatal error while storing job", err)
	}
	return
}
