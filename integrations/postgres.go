package integrations

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/viper"
)

type HandleT struct {
	dbHandle *sql.DB
}

func (cd *HandleT) Close() {
	cd.dbHandle.Close()
}

func (cd *HandleT) WriteWarehouse(jsonData []byte) bool {

	parsedJSON := make(map[string]interface{})
	// Unmarshal the JSON data into the map
	err := json.Unmarshal(jsonData, &parsedJSON)
	if err != nil {
		log.Fatal("Error decoding JSON:")
		return false
	}

	payloads, ok := parsedJSON["payload"].([]interface{})
	if !ok {
		log.Fatal("Error: 'payload' is not an array")
		return false
	}

	for _, payload := range payloads {
		data, ok := payload.(map[string]interface{})
		if !ok {
			log.Fatal("Error: 'payload' is not a map")
			return false
		}

		query := fmt.Sprintf(`INSERT INTO camunda_eventlog (	event_id, process_instance, task_name, task_type, task_seq, process_id, process_name, assignee_, task_start_time, task_end_time, task_duration ) VALUES ( '%s', '%s', '%s', '%s', %v, '%s', '%s', '%s', '%s', '%s', %v );`, data["event_id"],
			data["process_instance"],
			data["task_name"],
			data["task_type"],
			data["task_seq"],
			data["process_id"],
			data["process_name"],
			data["assignee_"],
			data["task_start_time"],
			data["task_end_time"],
			data["task_duration"])

		_, err = cd.dbHandle.Exec(query)
		if err != nil {
			log.Fatal(err)
			return false
		}
	}
	return true
}

func (cd *HandleT) Init() {
	cd.Setup()
}

func (cd *HandleT) Setup() {

	var err error

	psqlInfo := GetConnectionString()

	cd.dbHandle, err = sql.Open("postgres", psqlInfo)

	if err != nil {
		log.Fatal("Failed to open Warehouse DB connection", err)
	} else {
		log.Print("setup connection to to Warehouse DB")
	}

	cd.dbHandle.SetMaxIdleConns(5) // Set the maximum number of idle connections
	cd.dbHandle.SetMaxOpenConns(20)
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		viper.GetString("warehouse.host"),
		viper.GetString("warehouse.port"),
		viper.GetString("warehouse.user"),
		viper.GetString("warehouse.password"),
		viper.GetString("warehouse.name"))
}

func (cd *HandleT) CreateDestTable() {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS camunda_eventlog 
	(id BIGSERIAL PRIMARY KEY, event_id character varying, 
		process_instance character varying, 
		task_name character varying, 
		task_type character varying,
		task_seq bigint,
		process_id character varying,
		process_name character varying,
		assignee_ character varying,
		task_start_time timestamp without time zone,
		task_end_time timestamp without time zone,
		task_duration bigint
		 );`)
	_, err := cd.dbHandle.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}
