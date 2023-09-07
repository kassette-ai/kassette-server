package camunda

import (
	"fmt"
	"strconv"
	"time"
)

type TransformerHandleT struct {}

func (handleT *TransformerHandleT) toInt(val interface{}) (int, bool) {
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
		vnum, err := strconv.Atoi(val.(string))
		if err != nil {
			return 0, false
		} else {
			return vnum, true
		}
	case float32:
		return int(val.(float32)), true
	case float64:
		return int(val.(float64)), true
	default:
		return 0, false
	}
}

func (handleT *TransformerHandleT) toFloat(val interface{}) (float64, bool) {
	switch val.(type) {
	case int:
		return float64(val.(int)), true
	case int64:
		return float64(val.(int64)), true
	case bool:
		if val.(bool) {
			return 1, true
		} else {
			return 0, true
		}
	case string:
		vnum, err := strconv.ParseFloat(val.(string), 64)
		if err != nil{
			return 0, false
		} else {
			return vnum, true
		}
	case float32, float64:
		return val.(float64), true
	default:
		return 0, false
	}
}

func (handleT *TransformerHandleT) toBool(val interface{}) (bool, bool) {
	switch val.(type) {
	case string:
		return val.(string) == "true", true
	case bool:
		return val.(bool), true
	default:
		return false, false
	}
}

func (handleT *TransformerHandleT) toString(val interface{}) (string, bool) {
	return fmt.Sprintf("%v", val), true
}

func (handleT *TransformerHandleT) toDateTime(val interface{}) (time.Time, bool) {
	switch val.(type) {
	case string:
	default:
		return time.Time{}, false
	}
	parsedTime, err := time.Parse("2006-01-02T15:04:05.000Z", val.(string))
	if err != nil {
		return time.Time{}, false
	} else {
		return parsedTime, true
	}
}

func (handleT *TransformerHandleT) toDate(val interface{}) (time.Time, bool) {
	switch val.(type) {
	case string:
	default:
		return time.Time{}, false
	}
	parsedTime, err := time.Parse("2006-01-02T15:04:05.000Z", val.(string))
	if err != nil {
		return time.Time{}, false
	} else {
		return parsedTime, true
	}
}

func (handleT *TransformerHandleT) Convert(v interface{}, converToType string) (interface{}, bool) {
	var convertV interface{}
	var success bool
	switch converToType {
	case "string":
		convertV, success = handleT.toString(v)
	case "int":
		convertV, success = handleT.toInt(v)
	case "float64":
		convertV, success = handleT.toFloat(v)
	case "bool":
		convertV, success = handleT.toBool(v)
	case "date":
		convertV, success = handleT.toDate(v)
	case "datetime":
		convertV, success = handleT.toDateTime(v)
	}
	return convertV, success
}
