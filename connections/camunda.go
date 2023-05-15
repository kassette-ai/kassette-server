package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

// table act_hi_actinst
type ActivitiInstance struct {
	Id_                 string `json:"id_"`
	Parent_act_inst_id_ string `json:"parent_act_inst_id_"`
	Proc_def_key_       string `json:"proc_def_key_"`
	Proc_def_id_        string `json:"proc_def_id_"`
	Root_proc_inst_id_  string `json:"root_proc_inst_id_"`
	Proc_inst_id_       string `json:"proc_inst_id_"`
	Execution_id_       string `json:"execution_id_"`
	Act_id_             string `json:"act_id_"`
	Task_id_            string `json:"task_id_"`
	Call_proc_inst_id_  string `json:"call_proc_inst_id_"`
	Call_case_inst_id_  string `json:"call_case_inst_id_"`
	Act_name_           string `json:"act_name_"`
	Act_type_           string `json:"act_type_"`
	Assignee_           string `json:"assignee_"`
	Start_time_         string `json:"start_time_"`
	End_time_           string `json:"end_time_"`
	Duration_           int    `json:"duration_"`
	Act_inst_state_     int    `json:"act_inst_state_"`
	Sequence_counter_   int    `json:"sequence_counter_"`
	Tenant_id_          string `json:"tenant_id_"`
	Removal_time_       string `json:"removal_time_"`
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=%s",
		viper.GetString("database.host"),
		viper.GetString("database.port"),
		viper.GetString("database.user"),
		viper.GetString("database.password"),
		viper.GetString("database.name"),
		viper.GetString("database.ssl_mode"))
}

func startWorker(id string, name string, createdAt time.Time) {
	log.Fatal(fmt.Sprint("fetched record %s, with name %s at %s", id, name, createdAt)) // Do work here
}

func main() {
	// Load Config file
	viper.SetConfigFile("config.yaml")
	viper.SetConfigType("yaml")
	// Load configuration from environment variables
	viper.AutomaticEnv()

	verr := viper.ReadInConfig()
	if verr != nil {
		log.Println(verr)
		return
	}

	tableName := "act_hi_actinst"
	timestampCol := "start_time_"
	psqlInfo := GetConnectionString()
	var lastTimestamp time.Time

	log.Printf("Connecting to Database: %s\n", psqlInfo)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a ticker that polls the database every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Query the database for new records
			query := fmt.Sprintf("SELECT id_, act_name_, start_time_ FROM %s WHERE %s > $1", tableName, timestampCol)
			rows, err := db.QueryContext(context.Background(), query, lastTimestamp)
			if err != nil {
				log.Fatal(fmt.Sprintf("Error querying database: %v\n", err))
				continue
			}
			defer rows.Close()

			// Process the new records
			for rows.Next() {
				var activitiInstance ActivitiInstance
				var createdAt time.Time
				err := rows.Scan(&activitiInstance.Id_, &activitiInstance.Act_name_, &createdAt)
				if err != nil {
					log.Fatal(fmt.Sprintf("Error reading row: %v\n", err))
					continue
				}

				// Start a worker for the new record
				go startWorker(activitiInstance.Id_, activitiInstance.Act_name_, createdAt)

				// Update the last timestamp seen
				if createdAt.After(lastTimestamp) {
					lastTimestamp = createdAt
				}
			}
		}
	}
}
