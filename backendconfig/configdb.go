package backendconfig

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"
	"log"
	"reflect"
	"time"
)

var (
	configBackendURL, configBackendToken string
	pollInterval                         time.Duration
	curSourceJSON                        SourcesT
	initialized                          bool
)

var (
	Eb *utils.EventBus
)

type SourcesT struct {
	Sources []SourceT `json:"sources"`
}

func loadConfig() {
	pollInterval = 5 * time.Second
}

func init() {
	loadConfig()
}

func (cd *HandleT) pollConfigUpdate() {
	for {
		sourceJSON, ok := cd.getAllConfiguredSources()

		if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
			curSourceJSON = sourceJSON
			initialized = true
			Eb.Publish("backendconfig", sourceJSON)
		}
		time.Sleep(time.Duration(pollInterval))
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func Subscribe(channel chan utils.DataEvent) {
	Eb.Subscribe("backendconfig", channel)
	Eb.PublishToChannel(channel, "backendconfig", curSourceJSON)
}

func Update(channel chan utils.DataEvent, t SourceT) {
	updatePayload, _ := json.Marshal(t)
	Eb.PublishToChannel(channel, "update", updatePayload)
}

func WaitForConfig() {
	for {
		if initialized {
			break
		}
		time.Sleep(pollInterval)
	}
}

// Setup backend config
func (cd *HandleT) Init() {
	Eb = new(utils.EventBus)
	logger.Info("Initializing backend config")

	cd.Setup()
	initialized = true
	go cd.pollConfigUpdate()
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		viper.GetString("database.host"),
		viper.GetString("database.port"),
		viper.GetString("database.user"),
		viper.GetString("database.password"),
		viper.GetString("database.name"))
}

type HandleT struct {
	dbHandle        *sql.DB
	destinationList []DestinationT
	sourceList      []SourceT
	configList      SourcesT
}

func (cd *HandleT) Setup() {

	var err error

	psqlInfo := GetConnectionString()

	cd.dbHandle, err = sql.Open("postgres", psqlInfo)

	if err != nil {
		log.Fatal("Failed to open DB connection", err)
	}

	cd.createConfigTable()

}

func (cd *HandleT) getAllConfiguredSources() (sourceJSON SourcesT, ok bool) {

	sqlStatement := fmt.Sprintf(`SELECT id, source, write_key FROM source_config`)
	result, _ := cd.dbHandle.Prepare(sqlStatement)

	defer result.Close()

	rows, err := result.Query()

	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to read source_config table: %s", err.Error()))
		return
	}
	defer rows.Close()

	var sourceConfig SourcesT

	for rows.Next() {
		var id int
		var sourceString string
		var source SourceT
		var writeKey string

		err = rows.Scan(&id, &sourceString, &writeKey)

		json.Unmarshal([]byte(sourceString), &source)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to read source_config table: %s", err.Error()))
		}
		sourceConfig.Sources = append(sourceConfig.Sources, source)
	}

	return sourceConfig, true
}

func (cd *HandleT) createConfigTable() {

	var err error

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS source_config (
		id BIGSERIAL PRIMARY KEY,
		source JSONB NOT NULL,
		write_key VARCHAR(255) NOT NULL);`)

	_, err = cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create source_config table %s", err))
	}

	return
}

func (cd *HandleT) createSource(writeKey string, source SourceT) {

	var err error

	sourceJson, _ := json.Marshal(source)

	sqlStatement := fmt.Sprintf(`INSERT INTO source_config (source, write_key) VALUES ('%s', '%s')`, sourceJson, writeKey)

	_, err = cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create source_config table %s", err))
	}

	return
}
