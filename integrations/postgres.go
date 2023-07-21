package integrations

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"regexp"

	"github.com/lib/pq"
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

	//parsedJSON := gjson.ParseBytes(jsonData)
	//log.Printf("event log message: JSON parseddd,  %s", parsedJSON)
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

		kassette_data_type := data["kassette_data_type"]   //payload.Get("kassette_data_type").String()
		kassette_data_agent := data["kassette_data_agent"] //payload.Get("kassette_data_agent").String()
		// Define the regular expression pattern
		pattern := regexp.MustCompile(`^kassette_data_`)
		// Create a map to hold the JSON data
		//		data := make(map[string]string)
		// Unmarshal the JSON data into the map
		// err := json.Unmarshal(jsonData, &data)
		// if err != nil {
		// 	log.Fatal(fmt.Printf("Type of num: %T\n", payload))
		// 	return false
		// }
		//log.Fatal(fmt.Printf("Type of num: %T\n", data))
		// Create a slice to store the matching keys
		matchingKeys := make([]string, 0)
		matchingValues := make([]string, 0)
		// Iterate over the map and check if the key matches the pattern
		for key, value := range data {
			if !pattern.MatchString(key) {
				if str, ok := value.(string); ok {
					matchingKeys = append(matchingKeys, key)
					matchingValues = append(matchingValues, str)
				}
			}
		}

		if kassette_data_agent == "camunda" {
			query := fmt.Sprintf("INSERT INTO %s (", kassette_data_type)
			for i, key := range matchingKeys {
				query += pq.QuoteIdentifier(key)
				if i < len(matchingKeys)-1 {
					query += ","
				}
			}
			query += ") VALUES ("
			for i, value := range matchingValues {
				query += "'" + value + "'"
				if i < len(matchingValues)-1 {
					query += ","
				}
			}
			query += ")"

			//log.Printf(query)
			_, err = cd.dbHandle.Exec(query)
			if err != nil {
				log.Fatal(err)
				return false
			}
		} else {
			log.Fatal(fmt.Sprintf("Unknown Data Agent: %s", kassette_data_agent))
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

func (cd *HandleT) CreateDestTable(advancedConfig map[string]interface{}) {
	configs, ok := advancedConfig["source_config"].([]interface{})
	if !ok {
		log.Print("Error: 'source_config' is not an array")
		return
	}

	for _, config := range configs {
		data, ok := config.(map[string]interface{})
		if !ok {
			log.Print("Error: 'config' is not a map", data)
		}
		//log.Print(fmt.Sprintf("Table: %s, fields: %s", data["kassette_data_type"], data["config"]))
		table_config, ok := data["config"].([]interface{})
		if !ok {
			log.Fatal("Error: 'data[config]' is not a array", data["config"])
		}
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", data["kassette_data_type"])
		for i, value := range table_config {
			aa, ok := value.(map[string]interface{})
			if !ok {
				log.Fatal("Error: 'aa' is not a map", aa)
			}
			for a, b := range aa {
				bstr, ok := b.(string)
				if !ok {
					log.Fatal("Error: bstr is not a string: ", bstr)
				}
				query += a + " " + bstr
			}
			if i < len(table_config)-1 {
				query += ", "
			}
		}
		query += ");"
		//log.Print(fmt.Sprintf("Create Table statements: %s", query))
		_, err := cd.dbHandle.Exec(query)
		if err != nil {
			log.Fatal(err)
		}
	}
}
