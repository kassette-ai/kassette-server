package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

type TableRow map[string]interface{}

type Payload struct {
	Batch []map[string]interface{} `json:"batch"`
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
	maxAttempts := 20
	initialBackoff := 1 * time.Second
	maxBackoff := 10 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {

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
			log.Printf("Error sending request: %s", err)
			backoff := time.Duration(attempt-1) * initialBackoff
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			time.Sleep(backoff)
			continue
		}
		defer resp.Body.Close()
		// Check the response status code
		if resp.StatusCode != http.StatusOK {
			log.Fatal("Request failed with status:", resp.StatusCode)
			return
		}
		log.Printf("Request successful!\n")
		return
	}
	log.Fatal("Max retry attempts reached")
}

func startWorker(activitiInstances []map[string]interface{}) {
	// create the payload
	var payload Payload
	payload.Batch = activitiInstances

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("Json object: %s", string(jsonData))
	submitPayload(jsonData)
}

func get_new_records(dbHandler *sql.DB, tableName string, dbBatchSize string, trackColumn string, trackPosition time.Time, idColumn string, idExclude []string) (time.Time, []string, []map[string]interface{}) {
	var lastTrackPosition time.Time
	fetchedIds := make([]string, 0)
	query := fmt.Sprintf("SELECT * FROM %s where %s > $1 and %s not in ($2) limit %s;", tableName, trackColumn, idColumn, dbBatchSize)
	// Execute the SQL statement and retrieve the rows
	rows, err := dbHandler.QueryContext(context.Background(), query, trackPosition, strings.Join(idExclude, ", "))
	if err != nil {
		log.Fatal(err)
	}
	data := make([]map[string]interface{}, 0)

	// Iterate over the rows
	for rows.Next() {
		// Create a map to hold the row data
		record := make(TableRow)

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		// Create a slice to hold the column values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row values into the slice
		err = rows.Scan(values...)
		if err != nil {
			log.Fatal(err)
		}

		// Convert the row values into a JSON object
		for i, column := range columns {
			val := *(values[i].(*interface{}))
			record[column] = val
		}
		//Adding Kassette Metadata
		record["kassette_data_agent"] = "camunda"
		record["kassette_data_type"] = tableName

		fetchedId, ok := record[idColumn].(string)
		if !ok {
			log.Fatal("Invalid value type string")
		} else {
			fetchedIds = append(fetchedIds, fetchedId)
		}
		timestamp, ok := record[trackColumn].(time.Time)
		if !ok {
			log.Fatal("Invalid value type time.Time")
		} else {
			lastTrackPosition = timestamp
		}
		data = append(data, record)
	}
	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return lastTrackPosition, fetchedIds, data
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
	lastTimestamp := time.Now().Add(-2 * time.Hour) //start ingesting data 2 hours back after restart

	batchSubmit := make([]map[string]interface{}, 0)
	kassetteBatchSize := viper.GetInt("kassette-server.batch_size")
	//read tables settings into Map
	var trackTables map[string]map[string]string
	var trackTablesTs map[string]map[string]interface{}
	trackTablesTs = make(map[string]map[string]interface{})
	trackTables = make(map[string]map[string]string)
	for table, _ := range viper.GetStringMapString("tables") {
		trackTablesTs[table] = make(map[string]interface{})
		trackTables[table] = make(map[string]string)
		trackTablesTs[table]["lastTimestamp"] = lastTimestamp
		trackTablesTs[table]["lastFetched"] = make([]string, 0)
		for tableSettingKey, tableSettingValue := range viper.GetStringMapString("tables." + table) {
			trackTables[table][tableSettingKey] = tableSettingValue
		}
	}

	dbBatchSize := viper.GetString("database.batch_size")
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
			for table, tableData := range trackTables {
				lastFetched, ok := trackTablesTs[table]["lastFetched"].([]string)
				if !ok {
					log.Fatal("Type Error Array of strings")
				}
				lastLastTimestamp, lastLastFetched, batch := get_new_records(db, table, dbBatchSize, tableData["track_column"], lastTimestamp, tableData["id_column"], lastFetched)
				// Update the last seen timestamp of processed record
				// or store IDs of records belonging to the same timestamp to exclude them from the next select
				// to avoid duplication
				ts, ok := trackTablesTs[table]["lastTimestamp"].(time.Time)
				if !ok {
					log.Fatal("Type Error")
				}
				if lastLastTimestamp.After(ts) {
					trackTablesTs[table]["lastTimestamp"] = lastLastTimestamp
					trackTablesTs[table]["lastFetched"] = lastFetched[:0]
				} else {
					trackTablesTs[table]["lastFetched"] = append(lastFetched, lastLastFetched...)
				}
				batchSubmit = append(batchSubmit, batch...)
				if len(batchSubmit) >= kassetteBatchSize {
					startWorker(batchSubmit) //submit a batch if number of records enough
					batchSubmit = nil
				}
			}
			if len(batchSubmit) > 0 { //submit a batch if anything left after a cycle
				startWorker(batchSubmit)
				batchSubmit = nil
			}
		}
	}
}
