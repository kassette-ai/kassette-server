package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

type ActivitiInstance struct {
	Id_         int    `json:"id_"`
	Act_name_   string `json:"act_name_"`
	Start_time_ string `json:"start_time_"`
	End_time_   string `json:"end_time_"`
	Act_type_   string `json:"act_type_"`
	Assignee_   string `json:"assignee_"`
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

	psqlInfo := GetConnectionString()

	log.Printf("Connecting to Database: %s\n", psqlInfo)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare(fmt.Sprintf("SELECT * FROM act_hi_actinst WHERE id_ > $1;"))
	if err != nil {
		log.Fatal(err)
	}

	// Start the infinite loop
	lastID := 0
	for {
		// Query the table for new records
		rows, err := stmt.Query(lastID)
		if err != nil {
			log.Println(err)
			continue
		}

		// Process the new records
		for rows.Next() {
			var activitiInstance ActivitiInstance
			if err := rows.Scan(&activitiInstance.Id_, &activitiInstance.Act_name_, &activitiInstance.Start_time_, &activitiInstance.End_time_, &activitiInstance.Act_type_, &activitiInstance.Assignee_); err != nil {
				log.Println(err)
				continue
			}

			log.Printf("New record: id=%d, name=%s", activitiInstance.Id_, activitiInstance.Act_name_)
			if activitiInstance.Id_ > lastID {
				lastID = activitiInstance.Id_
			}

			// convert object to a JSON string
			jsonActivitiInstance, err := json.Marshal(activitiInstance)
			if err != nil {
				panic(err.Error())
			}

			// Send data to Kafka topic
			fmt.Println(string(jsonActivitiInstance))

		}

		// Close the rows object
		rows.Close()

		// Wait for a short period of time before polling again
		time.Sleep(5 * time.Second)
	}
}
