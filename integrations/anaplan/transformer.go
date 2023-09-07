package anaplan

import (
	"fmt"
	"strconv"
	"time"
)

type TransformerHandleT struct {}

func (handleT *TransformerHandleT) toNumber(val interface{}) (interface{}, bool) {
	switch val.(type) {
	case int:
		return val.(int), true
	case int64:
		return val.(int64), true
	case float32:
		return val.(float32), true
	case float64:
		return val.(float64), true
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

func (handleT *TransformerHandleT) toDate(val time.Time) (string, bool) {
	return val.Format("2006-01-02"), true
}

func (handleT *TransformerHandleT) Convert(v interface{}, converToType string) (interface{}, bool) {
	var convertV interface{}
	var success bool
	switch converToType {
	case "string":
		convertV, success = handleT.toString(v)
	case "number":
		convertV, success = handleT.toNumber(v)
	case "bool":
		convertV, success = handleT.toBool(v)
	case "date":
		convertV, success = handleT.toDate(v.(time.Time))
	}
	return convertV, success
}
