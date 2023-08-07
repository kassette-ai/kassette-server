package backendconfig

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/spf13/viper"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"
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
		time.Sleep(pollInterval)
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func Subscribe(channel chan utils.DataEvent) {
	Eb.Subscribe("backendconfig", channel)
	Eb.PublishToChannel(channel, "backendconfig", curSourceJSON)
}

func (cd *HandleT) Update(t SourceT, write_key string) {
	if cd.insertSource(write_key, t) {
		newSourceJSON, _ := json.Marshal(t)
		Eb.Publish("backendConfig", newSourceJSON)
	}
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
	cd.createServiceCatalogueTable()
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

func (cd *HandleT) createServiceCatalogueTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS service_catalogue (
		id BIGSERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		type VARCHAR(255) NOT NULL,
		access VARCHAR(255) NOT NULL,
		category VARCHAR(255) NOT NULL,
		url TEXT NOT NULL,
		notes TEXT NOT NULL,
		metadata JSONB,
		iconurl TEXT NOT NULL);`)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create service_catalogue table %s", err))
	}

	return
}

func (cd *HandleT) insertSource(writeKey string, source SourceT) bool {

	var err error

	sourceJson, _ := json.Marshal(source)

	sqlStatement := fmt.Sprintf(`INSERT INTO source_config (id, source, write_key) VALUES ('%s', '%s', '%s') 
                                                  ON CONFLICT (ID) DO UPDATE SET source = '%s', write_key = '%s'`,
		source.ID, sourceJson, writeKey, sourceJson, writeKey)

	_, err = cd.dbHandle.Exec(sqlStatement)

	if err != nil {	
		logger.Error(fmt.Sprintf("Failed to insert to source_config table %s", err))
		return false
	}
	return true
}

func (cd *HandleT) CreateNewServiceCatalogue(catalogue ServiceCatalogue) bool {

	sqlStatement := fmt.Sprintf(`INSERT INTO service_catalogue (name, type, access, category, url, notes, metadata, iconurl) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`,
			catalogue.Name, catalogue.Type, catalogue.Access, catalogue.Category, catalogue.Url, catalogue.Notes, catalogue.MetaData, catalogue.IconUrl)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to insert new service catalogue to service_catalogue table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) GetServiceCatalogue(service_type string) []ServiceCatalogue {

	sqlStatement := "Select id, name, type, access, category, url, notes, metadata, iconurl FROM service_catalogue"

	if service_type == "src" {
		sqlStatement += " where type='Source'";
	} else if service_type == "dest" {
		sqlStatement += " where type='Destination'";
	}

	logger.Info(sqlStatement)

	rows, err := cd.dbHandle.Query(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch from service catalogue table. Error: %s", err))
		return []ServiceCatalogue{}
	}

	catalogues := []ServiceCatalogue{}
	for rows.Next() {
		var cata ServiceCatalogue
		err := rows.Scan(&cata.ID, &cata.Name, &cata.Type, &cata.Access, &cata.Category, &cata.Url, &cata.Notes, &cata.MetaData, &cata.IconUrl)
		if err == nil {
			catalogues = append(catalogues, cata)
		}
	}
	return catalogues

}

func (cd *HandleT) DeleteServiceCatalogue(service_id string) bool {
	sqlStatement := fmt.Sprintf("DELETE from service_catalogue where id=%s", service_id)
	
	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete a service catalogue from service_catalogue table. Error: %s", err))
		return false
	}

	return true
}