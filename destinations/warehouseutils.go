package destinations

import (
	"fmt"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/config"
)

const (
	DestinationType = "destinationType"
)

const (
	RS             = "RS"
	BQ             = "BQ"
	SNOWFLAKE      = "SNOWFLAKE"
	POSTGRES       = "POSTGRES"
	CLICKHOUSE     = "CLICKHOUSE"
	MSSQL          = "MSSQL"
	AZURE_SYNAPSE  = "AZURE_SYNAPSE"
	DELTALAKE      = "DELTALAKE"
	S3_DATALAKE    = "S3_DATALAKE"
	GCS_DATALAKE   = "GCS_DATALAKE"
	AZURE_DATALAKE = "AZURE_DATALAKE"
)

type Warehouse struct {
	WorkspaceID string
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
	Type        string
	Identifier  string
}

func (w *Warehouse) GetBoolDestinationConfig(key string) bool {
	destConfig := w.Destination.Config
	if destConfig[key] != nil {
		if val, ok := destConfig[key].(bool); ok {
			return val
		}
	}
	return false
}

func GetConfigValue(key string, warehouse Warehouse) (val string) {
	configKey := fmt.Sprintf("Warehouse.pipeline.%s.%s.%s", warehouse.Source.ID, warehouse.Destination.ID, key)
	if config.IsSet(configKey) {
		return config.GetString(configKey, "")
	}
	destConfig := warehouse.Destination.Config
	if destConfig[key] != nil {
		val, _ = destConfig[key].(string)
	}
	return val
}
