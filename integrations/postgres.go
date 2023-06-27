package integrations

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"kassette.ai/kassette-server/utils/logger"
)

type HandleT struct {
	dbHandle *sql.DB
}

func (cd *HandleT) WriteWarehouse(jsonData []byte) bool {

	parsedJSON := gjson.ParseBytes(jsonData)
	log.Printf("event log message: JSON parseddd,  %s", parsedJSON)
	payloads := parsedJSON.Get("payload").Array()
	for _, payload := range payloads {
		activitiInstanceId := payload.Get("actinst_id_").String()
		activitiTaskName := payload.Get("actinst_act_name_").String()
		activitiCase := payload.Get("procinst_business_key_").String()
		startActivities := payload.Get("actinst_start_time_").Time()
		endActivities := payload.Get("actinst_end_time_").Time()
		eventdata := payload.String()
		// log.Printf("results: %s %s %s %s %s %s", activitiInstanceId, activitiTaskName, activitiCase, startActivities, endActivities, eventdata)
		_, err := cd.dbHandle.Exec("INSERT INTO eventlog (activiti_instance_id, activiti_task_name,	"+
			"activiti_case, start_activities, end_activities, eventdata ) VALUES "+
			"($1, $2, $3, $4, $5, $6)", activitiInstanceId, activitiTaskName, activitiCase, startActivities, endActivities, eventdata)
		if err != nil {
			log.Println("Failed to insert data:", err)
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

	cd.createDestTable()

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

func (cd *HandleT) createDestTable() {

	var err error

	sqlStatement := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS eventlog (
			id SERIAL PRIMARY KEY,
			activiti_instance_id VARCHAR,
			activiti_task_name VARCHAR,
			activiti_case VARCHAR,
			start_activities TIMESTAMP,
			end_activities TIMESTAMP,
			eventdata JSONB NOT NULL);`)

	_, err = cd.dbHandle.Exec(sqlStatement)

	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to create eventlog table %s", err))
	}

}
