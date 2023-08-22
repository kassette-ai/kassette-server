package postgres

import (
	"fmt"
	"time"
	"database/sql"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
)

type HandleT struct {
	DBHandle		*sql.DB			`json:"DBHandle"`
	TableName		string			`json:"TableName`
}

func(handle *HandleT) Connect(config string) bool {
	var configMap map[string]string
	err := json.Unmarshal([]byte(config), &configMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while establishing connections [%s]. Error: %s", config, err.Error()))
		return false
	}
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", configMap["host"], configMap["port"], configMap["user"], configMap["password"], configMap["database"], configMap["ssl_mode"])

	dbHandle, err := sql.Open("postgres", connectionString)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to open DB connection [%s]. Error: %s", config, err.Error()))
		return false
	}

	err = dbHandle.Ping()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to ping [%s]. Error: %s", config, err.Error()))
		return false
	}
	handle.DBHandle = dbHandle
	handle.TableName = "camunda_eventlog"
	logger.Info(fmt.Sprintf("Destination Connected [%s].", config))
	return true
}

func (handle *HandleT) InsertPayloadInTransaction(payloads []json.RawMessage) error {
	tx, err := handle.DBHandle.Begin()
	if err != nil {
		return err
	}
	for _, payload := range payloads {
		insertStmt := fmt.Sprintf(`INSERT INTO %s (payload, received_at) VALUES ($1, $2)`, handle.TableName)
		_, err = tx.Exec(insertStmt, payload, time.Now())
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	} else {
		return nil
	}
}
