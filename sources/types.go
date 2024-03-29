package sources

var (
	TypeMapKassetteToSrc = map[string]string{
		"INT":   	 	"int",
		"FLOAT":   	 	"float64",
		"BOOLEAN":   	"bool",
		"STRING": 	 	"string",
		"TIMESTAMP":	"datetime",
		"DATE":			"date",
	}
)

type TransformerHandleI interface {
	Convert(interface{}, string) (interface{}, bool)
}
