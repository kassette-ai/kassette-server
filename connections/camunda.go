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

type ActivitiInstance struct {
	Actinst_proc_inst_id_   sql.NullString `json:"actinst_proc_inst_id_"`
	Actinst_act_name_       sql.NullString `json:"actinst_act_name_"`
	Actinst_act_type_       sql.NullString `json:"actinst_act_type_"`
	Actinst_proc_def_key_   sql.NullString `json:"actinst_proc_def_key_"`
	Actinst_assignee_       sql.NullString `json:"actinst_assignee_"`
	Actinst_start_time_     sql.NullTime   `json:"actinst_start_time_"`
	Actinst_end_time_       sql.NullTime   `json:"actinst_end_time_"`
	Actinst_duration        sql.NullInt32  `json:"actinst_duration_"`
	Actinst_act_inst_state_ sql.NullString `json:"actinst_act_inst_state_"`
	Procdef_name_           sql.NullString `json:"procdef_name_"`
	Detail_type_            sql.NullString `json:"detail_type_"`
	Detail_var_type_        sql.NullString `json:"detail_var_type_"`
	Detail_name_            sql.NullString `json:"detail_name_"`
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
	log.Printf("fetched record %s, with name %s at %s", activitiInstance.Actinst_proc_inst_id_.String, activitiInstance.Actinst_act_name_.String, activitiInstance.Actinst_start_time_.Time.String())
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

	// tableName := "act_hi_actinst"
	// timestampCol := "start_time_"
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

			// query := fmt.Sprintf("SELECT id_,parent_act_inst_id_,proc_def_key_,proc_def_id_,root_proc_inst_id_,"+
			// 	"proc_inst_id_,execution_id_,act_id_,task_id_,call_proc_inst_id_,call_case_inst_id_,"+
			// 	"act_name_,act_type_,assignee_,start_time_,end_time_,duration_,"+
			// 	"act_inst_state_,sequence_counter_,tenant_id_,removal_time_ "+
			// 	"FROM %s WHERE %s > $1", tableName, timestampCol)

			query := fmt.Sprintf("select " +
				"actinst.proc_inst_id_," +
				"actinst.act_name_," +
				"actinst.act_type_," +
				"actinst.proc_def_key_," +
				"actinst.assignee_," +
				"actinst.start_time_," +
				"actinst.end_time_," +
				"actinst.duration_," +
				"actinst.act_inst_state_," +
				"procdef.name_," +
				"detail.type_," +
				"detail.var_type_," +
				"detail.name_ " +
				"from act_hi_actinst as actinst," +
				"act_re_procdef as procdef," +
				"act_hi_detail as detail " +
				"where actinst.start_time_ > $1 " +
				"and actinst.proc_def_key_=procdef.key_ and actinst.execution_id_=detail.act_inst_id_;")

			rows, err := db.QueryContext(context.Background(), query, lastTimestamp)
			if err != nil {
				log.Fatal(fmt.Sprintf("Error querying database: %v\n", err))
				continue
			}
			defer rows.Close()

			// Process the new records
			for rows.Next() {
				var activitiInstance ActivitiInstance
				err := rows.Scan(&activitiInstance.Actinst_proc_inst_id_,
					&activitiInstance.Actinst_act_name_,
					&activitiInstance.Actinst_act_type_,
					&activitiInstance.Actinst_proc_def_key_,
					&activitiInstance.Actinst_assignee_,
					&activitiInstance.Actinst_start_time_,
					&activitiInstance.Actinst_end_time_,
					&activitiInstance.Actinst_duration,
					&activitiInstance.Actinst_act_inst_state_,
					&activitiInstance.Procdef_name_,
					&activitiInstance.Detail_type_,
					&activitiInstance.Detail_var_type_,
					&activitiInstance.Detail_name_)

				if err != nil {
					log.Fatal(fmt.Sprintf("Error reading row: %v\n", err))
					continue
				}

				// Start a worker for the new record
				startWorker(activitiInstance)

				// Update the last timestamp seen
				if activitiInstance.Actinst_start_time_.Time.After(lastTimestamp) {
					lastTimestamp = activitiInstance.Actinst_start_time_.Time
				}
			}
		}
	}
}
