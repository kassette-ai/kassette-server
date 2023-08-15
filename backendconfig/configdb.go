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
	cd.createSourceTable()
	cd.createDestinationTable()
	cd.createConnectionTable()
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

func (cd *HandleT) createSourceTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS source (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			service_id INT REFERENCES service_catalogue(id),
			write_key TEXT NOT NULL,
			customer_id INT,
			config JSONB NOT NULL,
			status VARCHAR(255) NOT NULL);`)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create source table %s", err))
	}

	return
}

func (cd *HandleT) createDestinationTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS destination (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			service_id INT REFERENCES service_catalogue(id),
			customer_id INT,
			config JSONB NOT NULL,
			status VARCHAR(255) NOT NULL);`)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create destination table %s", err))
	}

	return
}

func (cd *HandleT) createConnectionTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS connection (
			id BIGSERIAL PRIMARY KEY,
			source_id INT REFERENCES source(id),
			destination_id INT REFERENCES destination(id),
			transforms JSONB NOT NULL);`)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create connection table %s", err))
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

func (cd *HandleT) CreateNewServiceCatalogue(catalogue ServiceCatalogueT) bool {

	sqlStatement := fmt.Sprintf(`INSERT INTO service_catalogue (name, type, access, category, url, notes, metadata, iconurl) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`,
			catalogue.Name, catalogue.Type, catalogue.Access, catalogue.Category, catalogue.Url, catalogue.Notes, catalogue.MetaData, catalogue.IconUrl)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to insert new service catalogue to service_catalogue table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) GetServiceCatalogue(service_type string) []ServiceCatalogueT {

	sqlStatement := "Select id, name, type, access, category, url, notes, metadata, iconurl FROM service_catalogue"

	if service_type == "src" {
		sqlStatement += " where type='Source'";
	} else if service_type == "dest" {
		sqlStatement += " where type='Destination'";
	}

	rows, err := cd.dbHandle.Query(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch from service catalogue table. Error: %s", err))
		return []ServiceCatalogueT{}
	}

	catalogues := []ServiceCatalogueT{}
	for rows.Next() {
		var cata ServiceCatalogueT
		err := rows.Scan(&cata.ID, &cata.Name, &cata.Type, &cata.Access, &cata.Category, &cata.Url, &cata.Notes, &cata.MetaData, &cata.IconUrl)
		if err == nil {
			catalogues = append(catalogues, cata)
		}
	}
	return catalogues

}

func (cd *HandleT) GetServiceCatalogueByID(ID int) (ServiceCatalogueT, error) {
	sqlStatement := fmt.Sprintf("Select id, name, type, access, category, url, notes, metadata, iconurl FROM service_catalogue where id = %d", ID)
	rows, err := cd.dbHandle.Query(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch from service catalogue table. Error: %s", err))
		return ServiceCatalogueT{}, err
	}

	var cata ServiceCatalogueT
	for rows.Next() {
		err := rows.Scan(&cata.ID, &cata.Name, &cata.Type, &cata.Access, &cata.Category, &cata.Url, &cata.Notes, &cata.MetaData, &cata.IconUrl)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to fetch from service catalogue table. Error: %s", err))
			return ServiceCatalogueT{}, err
		}
	}
	return cata, nil
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

func (cd *HandleT) CreateNewSource(source SourceInstanceT) bool {
	
	sqlStatement := fmt.Sprintf(`INSERT INTO source (name, service_id, write_key, customer_id, config, status) values('%s', %d, '%s', %d, '%s', '%s') RETURNING id`, source.Name, source.ServiceID, source.WriteKey, 1, source.Config, "enabled")

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create a new source into the source table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) GetAllSources() []SourceConnectionsT {
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, write_key, customer_id, config, status from source");

	rows, err := cd.dbHandle.Query(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch sources from source table. Error: %s", err))
		return []SourceConnectionsT{}
	}
	defer rows.Close()

	response := []SourceConnectionsT{}
	for rows.Next() {
		var source SourceInstanceT
		var sourceDetail SourceDetailT
		var sourceConnection SourceConnectionsT
		err = rows.Scan(&source.ID, &source.Name, &source.ServiceID, &source.WriteKey, &source.CustomerID, &source.Config, &source.Status)
		if err == nil {
			sourceDetail.Source = source
			cata, err := cd.GetServiceCatalogueByID(source.ServiceID)
			if err == nil {
				sourceDetail.Catalogue = cata
				sourceConnection.SourceDetail = sourceDetail
				sourceConnection.DestinationDetails = []DestinationDetailT{}
				response = append(response, sourceConnection)
			}
		}
	}
	return response
}

func (cd *HandleT) GetSourceByID(ID int) (SourceInstanceT, error) {
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, write_key, customer_id, config, status from source where id = %d", ID)

	rows, err := cd.dbHandle.Query(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch source from source table by ID. Error: %s", err))
		return SourceInstanceT{}, err
	}
	defer rows.Close()

	var source SourceInstanceT
	for rows.Next() {
		err = rows.Scan(&source.ID, &source.Name, &source.ServiceID, &source.WriteKey, &source.CustomerID, &source.Config, &source.Status)
		if err != nil {
			return SourceInstanceT{}, err
		}
	}
	
	return source, nil
}

func (cd *HandleT) Authenticate(hashValue string) (bool, error) {
	sqlStatement := fmt.Sprintf("SELECT count(*) from source where write_key = '%s'", hashValue)

	logger.Info(sqlStatement)

	rows, err := cd.dbHandle.Query(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch source from source table by ID. Error: %s", err))
		return false, err
	}
	defer rows.Close()

	count := 0

	for rows.Next() {
		err = rows.Scan(&count)

		if err != nil {
			logger.Error(fmt.Sprintf("Failed to fetch source from source table by ID. Error: %s", err))
			return false, err
		}
	}

	if count == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func (cd *HandleT) GetSourceDetailByID(ID int) (SourceDetailT, error) {
	var sourceDetail SourceDetailT
	source, err := cd.GetSourceByID(ID)
	if err != nil {
		return SourceDetailT{}, err
	}
	catalogue, err := cd.GetServiceCatalogueByID(source.ServiceID)
	if err != nil {
		return SourceDetailT{}, err
	}
	sourceDetail.Source = source
	sourceDetail.Catalogue = catalogue
	return sourceDetail, nil
}

func (cd *HandleT) UpdateSource(source SourceInstanceT) bool {
	sqlStatement := fmt.Sprintf("UPDATE source SET name='%s', service_id=%d, write_key='%s', customer_id=%d, config='%s', status='%s' where id = %d;", source.Name, source.ServiceID, source.WriteKey, source.CustomerID, source.Config, source.Status, source.ID);

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the source inside the source table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd* HandleT) DeleteSource(sourceID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from source where id = %d", sourceID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the source from the source table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd *HandleT) GetAllDestinations() []DestinationConnectionsT {
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, customer_id, config, status from destination");

	rows, err := cd.dbHandle.Query(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch destinations from destination table. Error: %s", err))
		return []DestinationConnectionsT{}
	}
	defer rows.Close()

	response := []DestinationConnectionsT{}
	for rows.Next() {
		var destination DestinationInstanceT
		var destinationDetail DestinationDetailT
		var destinationConnection DestinationConnectionsT
		err = rows.Scan(&destination.ID, &destination.Name, &destination.ServiceID, &destination.CustomerID, &destination.Config, &destination.Status)
		if err == nil {
			destinationDetail.Destination = destination
			cata, err := cd.GetServiceCatalogueByID(destination.ServiceID)
			if err == nil {
				destinationDetail.Catalogue = cata
				destinationConnection.DestinationDetail = destinationDetail
				destinationConnection.SourceDetails = []SourceDetailT{}
				response = append(response, destinationConnection)
			}
		}
	}
	return response
}

func (cd *HandleT) GetDestinationByID(ID int) (DestinationInstanceT, error) {
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, customer_id, config, status from destination where id = %d", ID)

	rows, err := cd.dbHandle.Query(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch destination from destination table by ID. Error: %s", err))
		return DestinationInstanceT{}, err
	}
	defer rows.Close()

	var destination DestinationInstanceT
	for rows.Next() {
		err = rows.Scan(&destination.ID, &destination.Name, &destination.ServiceID, &destination.CustomerID, &destination.Config, &destination.Status)
		if err != nil {
			return DestinationInstanceT{}, err
		}
	}
	
	return destination, nil
}

func (cd *HandleT) GetDestinationDetailByID(ID int) (DestinationDetailT, error) {
	var destinationDetail DestinationDetailT
	destination, err := cd.GetDestinationByID(ID)
	if err != nil {
		return DestinationDetailT{}, err
	}
	catalogue, err := cd.GetServiceCatalogueByID(destination.ServiceID)
	if err != nil {
		return DestinationDetailT{}, err
	}
	destinationDetail.Destination = destination
	destinationDetail.Catalogue = catalogue
	return destinationDetail, nil
}

func (cd *HandleT) CreateNewDestination(destination DestinationInstanceT) bool {
	sqlStatement := fmt.Sprintf(`INSERT INTO destination (name, service_id, customer_id, config, status) values('%s', %d, %d, '%s', '%s')`, destination.Name, destination.ServiceID, 1, destination.Config, "enabled")

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create a new destination into the destination table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd *HandleT) UpdateDestination(destination DestinationInstanceT) bool {
	sqlStatement := fmt.Sprintf("UPDATE destination SET name='%s', service_id=%d, customer_id=%d, config='%s', status='%s' where id = %d;", destination.Name, destination.ServiceID, destination.CustomerID, destination.Config, destination.Status, destination.ID);

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the destination inside the destination table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd* HandleT) DeleteDestination(destinationID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from destination where id = %d", destinationID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the destination from the destination table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd* HandleT) GetConnectionByID(ID int) (ConnectionInstanceT, error) {
	sqlStatement := fmt.Sprintf("SELECT id, source_id, destination_id, transforms FROM connection where id = %d", ID)

	rows, err := cd.dbHandle.Query(sqlStatement)
	
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch a connection from connection table. Error: %s", err))
		return ConnectionInstanceT{}, err
	}

	var connection ConnectionInstanceT
	for rows.Next() {
		err := rows.Scan(&connection.ID, &connection.SourceID, &connection.DestinationID, &connection.Transforms)
		if err != nil {
			return ConnectionInstanceT{}, nil
		}
	}

	return connection, nil
}

func (cd *HandleT) GetAllConnections() []ConnectionDetailT {
	sqlStatement := fmt.Sprintf("SELECT id, source_id, destination_id, transforms FROM connection")

	rows, err := cd.dbHandle.Query(sqlStatement)
	
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to fetch connections from connection table. Error: %s", err))
		return []ConnectionDetailT{}
	}

	response := []ConnectionDetailT{}
	for rows.Next() {
		var connectionDetail ConnectionDetailT
		var connection ConnectionInstanceT
		err := rows.Scan(&connection.ID, &connection.SourceID, &connection.DestinationID, &connection.Transforms)
		if err == nil {
			var sourceDetail SourceDetailT
			var destinationDetail DestinationDetailT
			sourceDetail, srcErr := cd.GetSourceDetailByID(connection.SourceID)
			destinationDetail, destErr := cd.GetDestinationDetailByID(connection.DestinationID)
			if srcErr == nil && destErr == nil {
				connectionDetail.Connection = connection
				connectionDetail.SourceDetail = sourceDetail
				connectionDetail.DestinationDetail = destinationDetail
				response = append(response, connectionDetail)
			}
		}
	}

	return response
}

func (cd *HandleT) CreateNewConnection(connection ConnectionInstanceT) bool {
	sqlStatement := fmt.Sprintf(`INSERT INTO connection (source_id, destination_id, transforms) values(%d, %d, '%s')`, connection.SourceID, connection.DestinationID, connection.Transforms)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create a new connection into the connection table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd *HandleT) UpdateConnection(connection ConnectionInstanceT) bool {
	sqlStatement := fmt.Sprintf("UPDATE connection SET source_id=%d, destination_id=%d, transforms='%s' where id = %d;", connection.SourceID, connection.DestinationID, connection.Transforms, connection.ID);

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the connection inside the connection table. Error: %s", err))
		return false
	}
	
	return true
}

func (cd* HandleT) DeleteConnection(connectionID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from connection where id = %d", connectionID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the connection from the connection table. Error: %s", err))
		return false
	}
	
	return true
}
