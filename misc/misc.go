package misc

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/bugsnag/bugsnag-go"
	"github.com/google/uuid"
	"reflect"
	"runtime/debug"
	"strings"
)

const (
	// RFC3339Milli with milli sec precision
	RFC3339Milli          = "2006-01-02T15:04:05.000Z07:00"
	NOTIMEZONEFORMATPARSE = "2006-01-02T15:04:05"
)

func GetTagName(id string, names ...string) string {
	var truncatedNames string
	for _, name := range names {
		name = strings.ReplaceAll(name, ":", "-")
		truncatedNames += TruncateStr(name, 15) + "_"
	}
	return truncatedNames + TailTruncateStr(id, 6)
}

func TruncateStr(str string, limit int) string {
	if len(str) > limit {
		str = str[:limit]
	}
	return str
}

func TailTruncateStr(str string, count int) string {
	if len(str) > count {
		str = str[len(str)-count:]
	}
	return str
}

func GetStringifiedData(data interface{}) string {
	if data == nil {
		return ""
	}
	switch d := data.(type) {
	case string:
		return d
	default:
		dataBytes, err := json.Marshal(d)
		if err != nil {
			return fmt.Sprint(d)
		}
		return string(dataBytes)
	}
}

func MapLookup(mapToLookup map[string]interface{}, keys ...string) interface{} {
	if len(keys) == 0 {
		return nil
	}
	if val, ok := mapToLookup[keys[0]]; ok {
		if len(keys) == 1 {
			return val
		}
		nextMap, ok := val.(map[string]interface{})
		if !ok {
			return nil
		}
		return MapLookup(nextMap, keys[1:]...)
	}
	return nil
}

// GetMD5UUID hashes the given string into md5 and returns it as auuid
func GetMD5UUID(str string) (uuid.UUID, error) {
	// To maintain backward compatibility, we are using md5 hash of the string
	// We are mimicking github.com/gofrs/uuid behavior:
	//
	// md5Sum := md5.Sum([]byte(str))
	// u, err := uuid.FromBytes(md5Sum[:])

	// u.SetVersion(uuid.V4)
	// u.SetVariant(uuid.VariantRFC4122)

	// google/uuid doesn't allow us to modify the version and variant
	// so we are doing it manually, using gofrs/uuid library implementation.
	md5Sum := md5.Sum([]byte(str))
	// SetVariant: VariantRFC4122
	md5Sum[8] = md5Sum[8]&(0xff>>2) | (0x02 << 6)
	// SetVersion: Version 4
	version := byte(4)
	md5Sum[6] = (md5Sum[6] & 0x0f) | (version << 4)

	return uuid.FromBytes(md5Sum[:])
}

// ParseRudderEventBatch looks for the batch structure inside event
func ParseKassetteEventBatch(eventPayload json.RawMessage) ([]interface{}, bool) {
	var eventListJSON map[string]interface{}
	err := json.Unmarshal(eventPayload, &eventListJSON)
	if err != nil {
		return nil, false
	}
	_, ok := eventListJSON["batch"]
	if !ok {
		return nil, false
	}
	eventListJSONBatchType, ok := eventListJSON["batch"].([]interface{})
	if !ok {
		return nil, false
	}
	return eventListJSONBatchType, true
}

// GetRudderEventVal returns the value corresponding to the key in the message structure
func GetKassetteEventVal(key string, kassetteEvent interface{}) (interface{}, bool) {

	rudderEventMap, ok := GetKassetteEventMap(kassetteEvent)
	if !ok {
		return nil, false
	}
	rudderVal, ok := rudderEventMap[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

// Contains returns true if an element is present in a iteratee.
// https://github.com/thoas/go-funk
func Contains(in interface{}, elem interface{}) bool {
	inValue := reflect.ValueOf(in)
	elemValue := reflect.ValueOf(elem)
	inType := inValue.Type()

	switch inType.Kind() {
	case reflect.String:
		return strings.Contains(inValue.String(), elemValue.String())
	case reflect.Map:
		for _, key := range inValue.MapKeys() {
			if equal(key.Interface(), elem) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < inValue.Len(); i++ {
			if equal(inValue.Index(i).Interface(), elem) {
				return true
			}
		}
	default:
		AssertError(fmt.Errorf("Type %s is not supported by Contains, supported types are String, Map, Slice, Array", inType.String()))
	}

	return false
}

func equal(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	return reflect.DeepEqual(expected, actual)

}

// AssertError panics if error
func AssertError(err error) {
	if err != nil {
		// debug.SetTraceback("all")
		debug.PrintStack()
		defer bugsnag.AutoNotify()
		panic(err)
	}
}

func GetKassetteEventUserID(eventList []interface{}) (string, bool) {
	userID, ok := GetKassetteEventVal("anonymousId", eventList[0])
	if !ok {
		return "", false
	}
	userIDStr, ok := userID.(string)
	return userIDStr, true
}

// GetRudderEventMap returns the event structure from the client payload
func GetKassetteEventMap(kassetteEvent interface{}) (map[string]interface{}, bool) {

	kassetteEventMap, ok := kassetteEvent.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return kassetteEventMap, true
}

// Assert panics if false
func Assert(cond bool) {
	if !cond {
		//debug.SetTraceback("all")
		debug.PrintStack()
		defer bugsnag.AutoNotify()
		panic("Assertion failed")
	}
}
