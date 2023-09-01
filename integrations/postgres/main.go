package postgres

import (
	"fmt"
	"strings"
	"reflect"
	"database/sql"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/processor"
)

type HandleT struct {
	DBHandle		*sql.DB						`json:"DBHandle"`
	Schema			processor.SchemaT			`json:"Schema"`
}

type BatchPayloadT struct {
	Payload			[]interface{}		`json:"payload"`
}

func (handle *HandleT) createDestinationTable(schema string) bool {
	var newSchema processor.SchemaT
	err := json.Unmarshal([]byte(schema), &newSchema)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while creating destination table [%s]. Error: %s", schema, err.Error()))
		return false
	}
	equal := reflect.DeepEqual(handle.Schema, newSchema)
	handle.Schema = newSchema
	if !equal {
		_, err := handle.DBHandle.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", handle.Schema.TableName))
		if err != nil {
			logger.Error(fmt.Sprintf("Error while dropping old destination table [%s]. Error: %s", schema, err.Error()))
			return false
		}
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

func (handle *HandleT) InsertPayloadInTransaction(rawPayloads []json.RawMessage) error {
	tx, err := handle.DBHandle.Begin()
	if err != nil {
		return err
	}
	var BatchPayloadMapList []BatchPayloadT
	payloads := []map[string]interface{}{}
	for _, rawPayload := range rawPayloads {
		json.Unmarshal(rawPayload, &BatchPayloadMapList)
		for _, batchPayload := range BatchPayloadMapList {
			for _, singleEvent := range batchPayload.Payload {
				payloads = append(payloads, singleEvent.(map[string]interface{}))
			}
		}
	}
	for _, payload := range payloads {

		valArr := []interface{}{}
		fieldNameArr := []string{}
		indexArr := []string{}
		index := 0
		for fieldName, val := range payload {
			if val != nil {
				fieldNameArr = append(fieldNameArr, fieldName)
				valArr = append(valArr, val)
				indexArr = append(indexArr, fmt.Sprintf("$%v", index + 1))
				index ++
			}
		}
		if index > 0 {
			insertStmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, handle.Schema.TableName, strings.Join(fieldNameArr, ","), strings.Join(indexArr, ","))
			_, err = tx.Exec(insertStmt, valArr...)
			if err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	} else {
		return nil
	}
}
