package postgres

import (
	"fmt"
	"encoding/json"
	"database/sql"
	"kassette.ai/kassette-server/utils/logger"
)

func Connect(config string) (*sql.DB, bool) {
	var configMap map[string]string
	err := json.Unmarshal([]byte(config), &configMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while establishing connections [%s]. Error: %s", config, err.Error()))
		return nil, false
	}
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", configMap["host"], configMap["port"], configMap["user"], configMap["password"], configMap["database"], configMap["ssl_mode"])

	dbHandle, err := sql.Open("postgres", connectionString)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to open DB connection [%s]. Error: %s", config, err.Error()))
		return nil, false
	}

	err = dbHandle.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to ping [%s]. Error: %s", config, err.Error()))
		return nil, false
	}
	return dbHandle, true
}
