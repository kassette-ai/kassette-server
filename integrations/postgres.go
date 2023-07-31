package integrations

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/lib/pq"
	"github.com/spf13/viper"
)

type HandleT struct {
	dbHandle *sql.DB
}

func (cd *HandleT) Close() {
	cd.dbHandle.Close()
}

func isNotNumber(str string) bool {
	_, err := strconv.Atoi(str)
	return err != nil
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

		task_seq := fmt.Sprintf("%v", data["task_seq"])
		if isNotNumber(task_seq) {
			task_seq = "NULL"
		}

		task_duration := fmt.Sprintf("%v", data["task_seq"])
		if isNotNumber(task_duration) {
			task_duration = "NULL"
		}

		var task_start_time pq.NullTime
		task_start_time_str, ok := data["task_start_time"].(string)
		if ok {
			timestamp, err := time.Parse("2006-01-02 15:04:05", task_start_time_str)
			if err == nil {
				task_start_time.Time = timestamp
				task_start_time.Valid = true
			} else {
				task_start_time.Valid = false
			}
		} else {
			log.Fatal("Problem with task_start_time")
		}

		var task_end_time pq.NullTime
		task_end_time_str, ok := data["task_end_time"].(string)
		if ok {
			timestamp, err := time.Parse("2006-01-02 15:04:05", task_end_time_str)
			if err == nil {
				task_end_time.Time = timestamp
				task_end_time.Valid = true
			} else {
				task_end_time.Valid = false
			}
		} else {
			log.Fatal("Problem with task_end_time")
		}

		query := fmt.Sprintf(`INSERT INTO camunda_eventlog (event_id, process_instance, task_name, task_type, task_seq, process_id, process_name, assignee_, task_start_time, task_end_time, task_duration ) VALUES ( '%s', '%s', '%s', '%s', %v, '%s', '%s', '%s',$1, $2, %v );`, data["event_id"],
			data["process_instance"],
			data["task_name"],
			data["task_type"],
			task_seq,
			data["process_id"],
			data["process_name"],
			data["assignee_"],
			task_duration)

		//log.Printf(query)

		_, err = cd.dbHandle.Exec(query, task_start_time, task_end_time)
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
