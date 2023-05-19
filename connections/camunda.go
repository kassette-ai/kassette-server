package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

// table act_hi_actinst
type ActivitiInstance struct {
	Id_                 sql.NullString `json:"id_"`
	Parent_act_inst_id_ sql.NullString `json:"parent_act_inst_id_"`
	Proc_def_key_       sql.NullString `json:"proc_def_key_"`
	Proc_def_id_        sql.NullString `json:"proc_def_id_"`
	Root_proc_inst_id_  sql.NullString `json:"root_proc_inst_id_"`
	Proc_inst_id_       sql.NullString `json:"proc_inst_id_"`
	Execution_id_       sql.NullString `json:"execution_id_"`
	Act_id_             sql.NullString `json:"act_id_"`
	Task_id_            sql.NullString `json:"task_id_"`
	Call_proc_inst_id_  sql.NullString `json:"call_proc_inst_id_"`
	Call_case_inst_id_  sql.NullString `json:"call_case_inst_id_"`
	Act_name_           sql.NullString `json:"act_name_"`
	Act_type_           sql.NullString `json:"act_type_"`
	Assignee_           sql.NullString `json:"assignee_"`
	Start_time_         sql.NullTime   `json:"start_time_"`
	End_time_           sql.NullTime   `json:"end_time_"`
	Duration_           sql.NullInt32  `json:"duration_"`
	Act_inst_state_     sql.NullInt32  `json:"act_inst_state_"`
	Sequence_counter_   sql.NullInt32  `json:"sequence_counter_"`
	Tenant_id_          sql.NullString `json:"tenant_id_"`
	Removal_time_       sql.NullString `json:"removal_time_"`
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

func submitPayload(jsonData []byte) {
	url := viper.GetString("kassette-server.url")
	uid := viper.GetString("kassette-agent.uid")

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatal("Error creating request:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("write_key", uid)
	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Error sending request:", err)
		return
	}
	defer resp.Body.Close()
	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		log.Fatal("Request failed with status:", resp.StatusCode)
		return
	}
	log.Printf("Request successful!\n")
}

func startWorker(activitiInstance ActivitiInstance) {
	// Do work here
	log.Printf("fetched record %s, with name %s at %s", activitiInstance.Act_id_.String, activitiInstance.Act_name_.String, activitiInstance.Start_time_.Time.String())
	jsonData, err := json.Marshal(activitiInstance)
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("Json object: %s", string(jsonData))
	submitPayload(jsonData)
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

			query := fmt.Sprintf("SELECT id_,parent_act_inst_id_,proc_def_key_,proc_def_id_,root_proc_inst_id_,"+
				"proc_inst_id_,execution_id_,act_id_,task_id_,call_proc_inst_id_,call_case_inst_id_,"+
				"act_name_,act_type_,assignee_,start_time_,end_time_,duration_,"+
				"act_inst_state_,sequence_counter_,tenant_id_,removal_time_ "+
				"FROM %s WHERE %s > $1", tableName, timestampCol)
			rows, err := db.QueryContext(context.Background(), query, lastTimestamp)
			if err != nil {
				log.Fatal(fmt.Sprintf("Error querying database: %v\n", err))
				continue
			}
			defer rows.Close()

			// Process the new records
			for rows.Next() {
				var activitiInstance ActivitiInstance
				err := rows.Scan(&activitiInstance.Id_,
					&activitiInstance.Parent_act_inst_id_,
					&activitiInstance.Proc_def_key_,
					&activitiInstance.Proc_def_id_,
					&activitiInstance.Root_proc_inst_id_,
					&activitiInstance.Proc_inst_id_,
					&activitiInstance.Execution_id_,
					&activitiInstance.Act_id_,
					&activitiInstance.Task_id_,
					&activitiInstance.Call_proc_inst_id_,
					&activitiInstance.Call_case_inst_id_,
					&activitiInstance.Act_name_,
					&activitiInstance.Act_type_,
					&activitiInstance.Assignee_,
					&activitiInstance.Start_time_,
					&activitiInstance.End_time_,
					&activitiInstance.Duration_,
					&activitiInstance.Act_inst_state_,
					&activitiInstance.Sequence_counter_,
					&activitiInstance.Tenant_id_,
					&activitiInstance.Removal_time_)
				if err != nil {
					log.Fatal(fmt.Sprintf("Error reading row: %v\n", err))
					continue
				}

				// Start a worker for the new record
				startWorker(activitiInstance)

				// Update the last timestamp seen
				if activitiInstance.Start_time_.Time.After(lastTimestamp) {
					lastTimestamp = activitiInstance.Start_time_.Time
				}
			}
		}
	}
}
