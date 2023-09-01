package integrations

type SchemaFieldT struct {
	Name			string				`json:"name"`
	Type			string				`json:"type"`
	PrimaryKey		bool				`json:"primary_key"`
}

type SchemaT struct {
	TableName		string				`json:"table_name"`
	SchemaFields	[]SchemaFieldT		`json:"schema_fields"`
}

type BatchPayloadT struct {
	Payload			[]interface{}		`json:"payload"`
}
