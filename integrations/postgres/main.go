package postgres

import (
	"fmt"
	"time"
	"strings"
	"database/sql"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
)

type SchemaFieldT struct {
	Name			string				`json:"name"`
	Type			string				`json:"type"`
	PrimaryKey		bool				`json:"primary_key"`
}

type SchemaT struct {
	TableName		string				`json:"table_name"`
	SchemaFields	[]SchemaFieldT		`json:"schema_fields"`
}

type HandleT struct {
	DBHandle		*sql.DB			`json:"DBHandle"`
	Schema			SchemaT			`json:"Schema"`
}

func (handle *HandleT) createDestinationTable(schema string) bool {
	err := json.Unmarshal([]byte(schema), &handle.Schema)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while creating destination table [%s]. Error: %s", schema, err.Error()))
		return false
	}
	createTableSql := `CREATE TABLE IF NOT EXISTS %s (%s);`
	fieldDefinitionList := []string{}
	for _, field := range handle.Schema.SchemaFields {
		fieldDefinition := field.Name + " " + field.Type
		if field.Type == "VARCHAR" {
			fieldDefinition +="(255)"
		}
		if field.PrimaryKey {
			fieldDefinition += " PRIMARY KEY"
		}
		fieldDefinitionList = append(fieldDefinitionList, fieldDefinition)
	}
	sqlStatement := fmt.Sprintf(createTableSql, handle.Schema.TableName, strings.Join(fieldDefinitionList, ","))
	_, err = handle.DBHandle.Exec(sqlStatement)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while creating destination table [%s]. Error: %s", schema, err.Error()))
		return false
	}
	return true
}

func (handle *HandleT) Connect(config string) bool {
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

	schemaStr, ok := configMap["schema"]
	if !ok {
		logger.Error(fmt.Sprintf("Can not find schema in the destination config. [%s]", config))
		return false
	}

	status := handle.createDestinationTable(schemaStr)

	if !status {
		return false;
	}

	logger.Info(fmt.Sprintf("Destination Connected [%s].", config))
	return true
}

func (handle *HandleT) InsertPayloadInTransaction(payloads []json.RawMessage) error {
	tx, err := handle.DBHandle.Begin()
	if err != nil {
		return err
	}
	for _, payload := range payloads {
		insertStmt := fmt.Sprintf(`INSERT INTO %s (payload, received_at) VALUES ($1, $2)`, handle.Schema.TableName)
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
