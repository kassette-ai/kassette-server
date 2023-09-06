package powerbi

import (
	"fmt"
	"strconv"
	"time"
)

type TransformerHandleT struct {}

func (handleT *TransformerHandleT) toNumber(val interface{}) (interface{}, bool) {
	switch val.(type) {
	case int, int64:
		return val.(int), true
	case bool:
		if val.(bool) {
			return 1, true
		} else {
			return 0, true
		}
	case string:
		vfloat, err := strconv.ParseFloat(val.(string), 64)
		if err == nil {
			return vfloat, true
		} else {
			vnum, err := strconv.Atoi(val.(string))
			if err != nil {
				return 0, false
			} else {
				return vnum, true
			}
		}
	case float32:
		return int(val.(float32)), true
	case float64:
		return int(val.(float64)), true
	default:
		return 0, false
	}
}

func (handleT *TransformerHandleT) toString(val interface{}) (string, bool) {
	return fmt.Sprintf("%v", val), true
}

func (handleT *TransformerHandleT) toDateTime(val time.Time) (string, bool) {
	return val.Format("2006-01-02T15:04:05.000Z"), true
}

func (handleT *TransformerHandleT) Convert(v interface{}, converToType string) (interface{}, bool) {
	var convertV interface{}
	var success bool
	switch converToType {
	case "string":
		convertV, success = handleT.toString(v)
	case "number":
		convertV, success = handleT.toNumber(v)
	case "datetime":
		convertV, success = handleT.toDateTime(v.(time.Time))
	}
	return convertV, success
}
