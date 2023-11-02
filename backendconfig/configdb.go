package backendconfig

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"kassette.ai/kassette-server/utils"
	"kassette.ai/kassette-server/utils/logger"
)

var (
	configBackendURL, configBackendToken string
	pollInterval                         time.Duration
	curSourceJSON                        ConnectionDetailsT
	initialized                          bool
)

var (
	Eb *utils.EventBus
)

func loadConfig() {
	pollInterval = 5 * time.Second
}

func init() {
	loadConfig()
}

func GetConfig() ConnectionDetailsT {
	return curSourceJSON
}

func Subscribe(channel chan utils.DataEvent) {
	Eb.Subscribe("backendconfig", channel)
	Eb.PublishToChannel(channel, "backendconfig", curSourceJSON)
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

// Setup backend config
func (cd *HandleT) Init() {
	Eb = new(utils.EventBus)

	logger.Info("Initializing backend config")
	cd.Setup()
}

func (cd *HandleT) Setup() {

	var err error

	psqlInfo := GetConnectionString()

	cd.dbHandle, err = sql.Open("postgres", psqlInfo)

	if err != nil {
		log.Fatal("Failed to open DB connection", err)
	}

	//cd.CreateServiceCatalogueTable()
	cd.CreateSourceTable()
	cd.CreateDestinationTable()
	cd.CreateConnectionTable()

	go cd.PollConfigUpdate()
}

func (cd *HandleT) PollConfigUpdate() {
	for {
		var newSourceJSON ConnectionDetailsT
		newSourceJSON.Connections = cd.GetAllConnections()
		if !reflect.DeepEqual(curSourceJSON, newSourceJSON) {
			initialized = true
			curSourceJSON = newSourceJSON
			Eb.Publish("backendconfig", newSourceJSON)
		}
		time.Sleep(pollInterval)
	}
}

func (cd *HandleT) CreateServiceCatalogueTable() {

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

func (cd *HandleT) CreateSourceTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS source (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			service_id INT,
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

func (cd *HandleT) CreateDestinationTable() {

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS destination (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			service_id INT,
			customer_id INT,
			config JSONB NOT NULL,
			status VARCHAR(255) NOT NULL);`)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create destination table %s", err))
	}

	return
}

func (cd *HandleT) CreateConnectionTable() {

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

	catalogues := []ServiceCatalogueT{}
	yamlData, err := os.ReadFile("schemas/catalogue.yaml")
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	err = yaml.Unmarshal(yamlData, &catalogues)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}

	var cataloguesType []ServiceCatalogueT
	var catalogueType string

	if service_type == "src" {
		catalogueType = "Source"
	} else if service_type == "dest" {
		catalogueType = "Destination"
	}

	for _, item := range catalogues {
		if item.Type == catalogueType {
			cataloguesType = append(cataloguesType, item)
		}
	}
	return cataloguesType

}

func (cd *HandleT) GetServiceCatalogueByID(ID int) (ServiceCatalogueT, error) {

	catalogues := []ServiceCatalogueT{}
	yamlData, err := os.ReadFile("schemas/catalogue.yaml")
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	err = yaml.Unmarshal(yamlData, &catalogues)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}

	var cata ServiceCatalogueT

	for _, item := range catalogues {
		if item.ID == ID {
			cata = item
			break
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
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, write_key, customer_id, config, status from source")

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
	sqlStatement := fmt.Sprintf("UPDATE source SET name='%s', service_id=%d, write_key='%s', customer_id=%d, config='%s', status='%s' where id = %d;", source.Name, source.ServiceID, source.WriteKey, source.CustomerID, source.Config, source.Status, source.ID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the source inside the source table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) DeleteSource(sourceID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from source where id = %d", sourceID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the source from the source table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) GetAllDestinations() []DestinationConnectionsT {
	sqlStatement := fmt.Sprintf("SELECT id, name, service_id, customer_id, config, status from destination")

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
	sqlStatement := fmt.Sprintf("UPDATE destination SET name='%s', service_id=%d, customer_id=%d, config='%s', status='%s' where id = %d;", destination.Name, destination.ServiceID, destination.CustomerID, destination.Config, destination.Status, destination.ID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the destination inside the destination table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) DeleteDestination(destinationID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from destination where id = %d", destinationID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the destination from the destination table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) GetConnectionByID(ID int) (ConnectionInstanceT, error) {
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
	sqlStatement := fmt.Sprintf("UPDATE connection SET source_id=%d, destination_id=%d, transforms='%s' where id = %d;", connection.SourceID, connection.DestinationID, connection.Transforms, connection.ID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update the connection inside the connection table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) DeleteConnection(connectionID int) bool {
	sqlStatement := fmt.Sprintf("DELETE from connection where id = %d", connectionID)

	_, err := cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete the connection from the connection table. Error: %s", err))
		return false
	}

	return true
}

func (cd *HandleT) Authenticate(hashValue string) (bool, error) {
	sqlStatement := fmt.Sprintf("SELECT count(*) from source where write_key = '%s'", hashValue)

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
