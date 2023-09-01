package processor

import (
	"fmt"
	"sort"
	"sync"
	"time"
	"strconv"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/integrations"
	"kassette.ai/kassette-server/integrations/postgres"
	"kassette.ai/kassette-server/integrations/powerbi"
	"kassette.ai/kassette-server/integrations/anaplan"
)

var (
	TransType = map[string]string{
		"FIELDMAP": "field_map",
		"FIELDHIDING": "field_hide",
		"FIELDDELETING": "field_delete",
	}
	SystemTransformationRules = []TransformationRuleT{
		{
			Field: "anonymousId",
			Type: TransType["FIELDHIDING"],
		},
	}
)	

type TransformationRuleT struct {
	From		string		`json:"from"`
	To			string		`json:"to"`
	Field		string		`json:"field"`
	Type		string		`json:"type"`
	Value		string		`json:"value"`
}

type ResponseT struct {
	Events       []interface{}
	Success      bool
}

type transformerHandleT struct {
	requestQ   chan *transformMessageT
	responseQ  chan *transformMessageT
	accessLock sync.Mutex
}

type transformMessageT struct {
	index 				int
	data  				interface{}
	rules  				[]TransformationRuleT
	schema				integrations.SchemaT
	srcCataName			string
	destCataName		string
}

func convertToString(val interface{}) (string, bool) {
	return fmt.Sprintf("%v", val), true
}

func convertToInt(val interface{}) (int, bool) {
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

func convertToFloat(val interface{}) (float64, bool) {
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

func convertToBool(val interface{}) (bool, bool) {
	switch val.(type) {
	case string:
		return val.(string) == "true", true
	case bool:
		return val.(bool), true
	default:
		return false, false
	}
}

func convertToNumber(val interface{}) (interface{}, bool) {
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

func convertToDate(val interface{}, srcName string, destName string) (string, bool) {
	switch val.(type) {
	case string:
	default:
		return "", false
	}
	
	var srcTime time.Time
	switch srcName {
	case "Camunda":
		parsedTime, err := time.Parse("2006-01-02T15:04:05.000Z", val.(string))
		if err != nil {
			logger.Info(fmt.Sprintf("Error!!!!!: %s", err.Error()))
			return "", false
		} else {
			srcTime = parsedTime
		}
	}
	if srcTime.IsZero() {
		return "", false
	}
	
	switch destName {
	case "Anaplan", "Postgres":
		return srcTime.Format("2006-01-02"), true
	}
	
	return "", true
}

func convertToDateTime(val interface{}, srcName string, destName string) (string, bool) {
	
	switch val.(type) {
	case string:
	default:
		return "", false
	}
	
	var srcTime time.Time
	switch srcName {
	case "Camunda":
		parsedTime, err := time.Parse("2006-01-02T15:04:05.000Z", val.(string))
		if err != nil {
			logger.Info(fmt.Sprintf("Error!!!!!: %s", err.Error()))
			return "", false
		} else {
			srcTime = parsedTime
		}
	}
	if srcTime.IsZero() {
		return "", false
	}
	
	switch destName {
	case "PowerBI", "Postgres":
		return srcTime.Format("2006-01-02T15:04:05.000Z"), true
	}
	
	return "", true
}

func (trans *transformerHandleT) transformWorker() {

	for job := range trans.requestQ {
		reqArray := job.data.([]interface{})
		respArray := transformBatchPayload(reqArray, job.rules, job.schema, job.srcCataName, job.destCataName)
		trans.responseQ <- &transformMessageT{data: respArray, index: job.index}
	}
}

func transformBatchPayload(m []interface{}, rules []TransformationRuleT, destSchema integrations.SchemaT, srcCataName string, destCataName string) map[string]interface{} {

	var typeMapKassetteToDest map[string]string
	rawTransform := make(map[string]interface{})
	batchPayload := make([]interface{}, 0)

	switch destCataName {
	case "Postgres":
		typeMapKassetteToDest = postgres.TypeMapKassetteToDest
	case "PowerBI":
		typeMapKassetteToDest = powerbi.TypeMapKassetteToDest
	case "Anaplan":
		typeMapKassetteToDest = anaplan.TypeMapKassetteToDest
	}

	for _, rawMap := range m {

		rawMap := rawMap.(map[string]interface{})
		rawPayload := rawMap["message"].(map[string]interface{})
		transformedPayload := make(map[string]interface{})

		delete := false
		for k, v := range rawPayload {
			fieldName := k
			hide := false
			for _, rule := range rules {
				if rule.Type == TransType["FIELDMAP"] {
					if rule.From == k {
						fieldName = rule.To
					}
				} else if rule.Type == TransType["FIELDHIDING"] {
					if rule.Field == k {
						hide = true
					}
				} else if rule.Type == TransType["FIELDDELETING"] {
					switch v.(type) {
					case int:
						vnum, err := strconv.Atoi(rule.Value)
						if err == nil && vnum == v.(int) {
							delete = true
						}
					case string:
						if rule.Value == v.(string) {
							delete = true
						}
					case bool:
						if rule.Value == "true" && v.(bool) || rule.Value == "false" && !v.(bool) {
							delete = true
						}
					}
				}
			}
			if !hide && typeMapKassetteToDest != nil{
				var destDataType string
				destFields := destSchema.SchemaFields
				if len(destFields) == 0 {
					if destCataName != "Postgres" {
						transformedPayload[fieldName] = v
					}
				} else {
					for _, destfield := range destFields {
						if destfield.Name == fieldName {
							destDataType = destfield.Type
							break
						}
					}
					if destDataType != "" {
						destType, ok := typeMapKassetteToDest[destDataType]
						if ok {
							var convertV interface{}
							var success bool
							switch destType {
							case "string":
								convertV, success = convertToString(v)
							case "int":
								convertV, success = convertToInt(v)
							case "float":
								convertV, success = convertToFloat(v)
							case "bool":
								convertV, success = convertToBool(v)
							case "number":
								convertV, success = convertToNumber(v)
							case "date":
								convertV, success = convertToDate(v, srcCataName, destCataName)
							case "datetime":
								convertV, success = convertToDateTime(v, srcCataName, destCataName)
							}
							if success {
								transformedPayload[fieldName] = convertV
							} else {
								transformedPayload[fieldName] = nil
							}
						}
					}
				}
			}
		}
		if !delete {
			batchPayload = append(batchPayload, transformedPayload)
		}
	}

	// Converting to array to support PowerBi
	rawTransform["payload"] = batchPayload

	logger.Debug(fmt.Sprintf("Transformed payload: %v", rawTransform))

	return rawTransform
}

// Transform function is used to invoke transformer API
// Transformer is not thread safe. So we need to create a new instance for each request
func (trans *transformerHandleT) Transform(clientEvents []interface{}, ruleStr string, config string, srcCataName string, destCataName string, batchSize int) ResponseT {

	logger.Info("Transform!!!!")

	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()

	var rules []TransformationRuleT
	err := json.Unmarshal([]byte(ruleStr), &rules)
	if err != nil {
		logger.Debug(fmt.Sprintf("Error while unmarshaling transformation rules: %s", err.Error()))
		return ResponseT{
			Events:       []interface{}{},
			Success:      false,
		}
	}

	rules = append(rules, SystemTransformationRules...)

	var configMap map[string]interface{}
	err = json.Unmarshal([]byte(config), &configMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", config, err.Error()))
		return ResponseT{
			Events: 		[]interface{}{},
			Success: 		false,
		}
	}

	schema := integrations.SchemaT{}

	schemaStr, ok := configMap["schema"].(string)
	if ok {
		err = json.Unmarshal([]byte(schemaStr), &schema)
		if err != nil {
			logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", schemaStr, err.Error()))
			return ResponseT {
				Events:			[]interface{}{},
				Success:		false,
			}
		}
	}

	var transformResponse = make([]*transformMessageT, 0)
	inputIdx := 0
	outputIdx := 0
	totalSent := 0
	reqQ := trans.requestQ
	resQ := trans.responseQ

	var toSendData interface{}

	for {
		//The channel is still live and the last batch has been sent
		//Construct the next batch
		if reqQ != nil && toSendData == nil {
			if batchSize > 0 {
				clientBatch := make([]interface{}, 0)
				batchCount := 0
				for {
					if batchCount >= batchSize || inputIdx >= len(clientEvents) {
						break
					}
					clientBatch = append(clientBatch, clientEvents[inputIdx])
					batchCount++
					inputIdx++
				}
				toSendData = clientBatch
			} else {
				toSendData = clientEvents[inputIdx]
				inputIdx++
			}
		}
		select {
		//In case of batch event, index is the next Index
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, rules: rules, schema: schema, srcCataName: srcCataName, destCataName: destCataName}:
			totalSent++
			toSendData = nil
			if inputIdx == len(clientEvents) {
				reqQ = nil
			}
		case data := <-resQ:
			transformResponse = append(transformResponse, data)
			outputIdx++
			//If all was sent and all was received we are done
			if reqQ == nil && outputIdx == totalSent {
				resQ = nil
			}
		}
		if reqQ == nil && reqQ != nil && len(reqQ) == 0 {
			break
		}
		if reqQ == nil && resQ == nil {
			break
		}
	}

	logger.Info(fmt.Sprintf("Sort events"))

	//Sort the responses in the same order as input
	sort.Slice(transformResponse, func(i, j int) bool {
		return transformResponse[i].index < transformResponse[j].index
	})

	outClientEvents := make([]interface{}, 0)

	for _, resp := range transformResponse {
		if resp.data == nil {
			continue
		}
		outClientEvents = append(outClientEvents, resp.data)
	}

	return ResponseT{
		Events:       outClientEvents,
		Success:      true,
	}
}

func (trans *transformerHandleT) Setup() {
	trans.requestQ = make(chan *transformMessageT, 100)
	trans.responseQ = make(chan *transformMessageT, 100)

	for i := 0; i < 5; i++ {
		logger.Info(fmt.Sprintf("Starting transformer worker", i))
		go trans.transformWorker()
	}
}
