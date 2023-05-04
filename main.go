package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"log"
	"os"
	"time"
)

func main() {
	viper.SetConfigFile(".env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Println(err)
		return
	}

	// source environment variables
	//setupPostgres()
	var gatewayDB jobsdb.HandleT
	var gateway gateway.HandleT

	gateway.Setup(&gatewayDB)

}

func setupPostgres() {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresUser := os.Getenv("POSTGRES_USER")
	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	postgresPort := os.Getenv("POSTGRES_PORT")
	if postgresPort == "" {
		postgresPort = "5432"
	}
	postgresDb := os.Getenv("POSTGRES_DB")
	postgresSslMode := os.Getenv("POSTGRES_SSL_MODE")
	if postgresSslMode == "" {
		postgresSslMode = "disable"
	}
	kafkaUrl := os.Getenv("KAFKA_URL")
	// source environment variables
	// define data objects
	type ActivitiInstance struct {
		Id_         int    `json:"id_"`
		Act_name_   string `json:"act_name_"`
		Start_time_ string `json:"start_time_"`
		End_time_   string `json:"end_time_"`
		Act_type_   string `json:"act_type_"`
		Assignee_   string `json:"assignee_"`
	}

	// Set up connection to PostgreSQL database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", postgresHost, postgresPort, postgresUser, postgresPassword, postgresDb, postgresSslMode))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set up connection to Kafka broker
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaUrl})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Set up Kafka topic
	topic := "workflow"

	// Set up date range for data query
	// startDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	// endDate := time.Date(2022, 1, 31, 23, 59, 59, 0, time.UTC)

	// Prepare the SQL statement for polling the table
	stmt, err := db.Prepare(fmt.Sprintf("SELECT * FROM act_hi_actinst WHERE id > $1;"))
	if err != nil {
		log.Fatal(err)
	}

	// Start polling
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
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(jsonActivitiInstance),
			}, nil)

			if err != nil {
				log.Printf("Failed to produce message: %v", err)
			}

		}

		// Close the rows object
		rows.Close()

		// Wait for a short period of time before polling again
		time.Sleep(5 * time.Second)
		// Wait for Kafka producer to flush messages to broker
		p.Flush(15 * 1000)
	}
}
