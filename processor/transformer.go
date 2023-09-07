package processor

import (
	"fmt"
	"sort"
	"sync"
	"strconv"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
	"kassette.ai/kassette-server/integrations"
	"kassette.ai/kassette-server/sources"
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
	index 					int
	data  					interface{}
	rules  					[]TransformationRuleT
	destSchema				integrations.SchemaT
	destConverter			integrations.TransformerHandleI
	typeMapToDest			map[string]string
	destSkipWithNoSchema	bool
	srcSchema				integrations.SchemaT
	srcConverter			sources.TransformerHandleI
	typeMapToSrc			map[string]string
	srcSkipWithNoSchema		bool
}

func (trans *transformerHandleT) transformWorker() {

	for job := range trans.requestQ {
		reqArray := job.data.([]interface{})
		respArray := transformBatchPayload(reqArray, job.rules, job.destConverter, job.destSchema, job.typeMapToDest, job.destSkipWithNoSchema, job.srcConverter, job.srcSchema, job.typeMapToSrc, job.srcSkipWithNoSchema)
		trans.responseQ <- &transformMessageT{data: respArray, index: job.index}
	}
}

func transformBatchPayload(m []interface{}, rules []TransformationRuleT, destConverter integrations.TransformerHandleI, destSchema integrations.SchemaT, typeMapKassetteToDest map[string]string, destSkipWithNoSchema bool, srcConverter sources.TransformerHandleI, srcSchema integrations.SchemaT, typeMapKassetteToSrc map[string]string, srcSkipWithNoSchema bool) map[string]interface{} {

	rawTransform := make(map[string]interface{})
	batchPayload := make([]interface{}, 0)

	for _, rawMap := range m {

		rawMap := rawMap.(map[string]interface{})
		rawPayload := rawMap["message"].(map[string]interface{})
		transformedPayload := make(map[string]interface{})

		delete := false
		for k, v := range rawPayload {
			var convertV interface{}
			var sourceConvSuccess bool
			if typeMapKassetteToSrc != nil {
				var srcDataType string
				srcFields := srcSchema.SchemaFields
				if len(srcFields) == 0 {
					if !srcSkipWithNoSchema {
						convertV = v
						sourceConvSuccess = true
					} else {
						convertV = nil
						sourceConvSuccess = false
					}
				} else {
					for _, srcField := range srcFields {
						if srcField.Name == k {
							srcDataType = srcField.Type
							break
						}
					}
					if srcDataType != "" {
						srcType, ok := typeMapKassetteToSrc[srcDataType]
						if ok {
							convertV, sourceConvSuccess = srcConverter.Convert(v, srcType)
						} else {
							convertV = nil
							sourceConvSuccess = false	
						}
					} else {
						convertV = nil
						sourceConvSuccess = false
					}
				}
			}
			if !sourceConvSuccess {
				continue
			}
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
					switch convertV.(type) {
					case int:
						vnum, err := strconv.Atoi(rule.Value)
						if err == nil && vnum == convertV.(int) {
							delete = true
						}
					case string:
						if rule.Value == convertV.(string) {
							delete = true
						}
					case bool:
						if rule.Value == "true" && convertV.(bool) || rule.Value == "false" && !convertV.(bool) {
							delete = true
						}
					}
				}
			}
			if !hide && typeMapKassetteToDest != nil {
				var destDataType string
				destFields := destSchema.SchemaFields
				if len(destFields) == 0 {
					if !destSkipWithNoSchema {
						transformedPayload[fieldName] = convertV
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
							logger.Info(fmt.Sprintf("heyheyhey:!!!! %v %v", convertV, destType))
							destConvertedV, success := destConverter.Convert(convertV, destType)
							if success {
								transformedPayload[fieldName] = destConvertedV
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
func (trans *transformerHandleT) Transform(clientEvents []interface{}, ruleStr string, destConfig string, destConverter integrations.TransformerHandleI, typeMapToDest map[string]string, destSkipWithNoSchema bool, srcConfig string, srcConverter sources.TransformerHandleI, typeMapToSrc map[string]string, srcSkipWithNoSchema bool, batchSize int) ResponseT {

	logger.Info("Transform!!!!")

	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()

	var rules []TransformationRuleT
	var srcConfigMap map[string]interface{}
	var destConfigMap map[string]interface{}
	
	err := json.Unmarshal([]byte(ruleStr), &rules)
	if err != nil {
		logger.Debug(fmt.Sprintf("Error while unmarshaling transformation rules: %s", err.Error()))
		return ResponseT{
			Events:       []interface{}{},
			Success:      false,
		}
	}
	rules = append(rules, SystemTransformationRules...)

	err = json.Unmarshal([]byte(srcConfig), &srcConfigMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", srcConfig, err.Error()))
		return ResponseT{
			Events: 		[]interface{}{},
			Success: 		false,
		}
	}
	srcSchema := integrations.SchemaT{}
	srcSchemaStr, ok := srcConfigMap["schema"].(string)
	if ok {
		err = json.Unmarshal([]byte(srcSchemaStr), &srcSchema)
		if err != nil {
			logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", srcSchemaStr, err.Error()))
			return ResponseT {
				Events:			[]interface{}{},
				Success:		false,
			}
		}
	}

	err = json.Unmarshal([]byte(destConfig), &destConfigMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", destConfig, err.Error()))
		return ResponseT{
			Events: 		[]interface{}{},
			Success: 		false,
		}
	}
	destSchema := integrations.SchemaT{}
	destSchemaStr, ok := destConfigMap["schema"].(string)
	if ok {
		err = json.Unmarshal([]byte(destSchemaStr), &destSchema)
		if err != nil {
			logger.Error(fmt.Sprintf("Error while getting schema for transformation [%s]. Error: %s", destSchemaStr, err.Error()))
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
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, rules: rules, destConverter: destConverter, destSchema: destSchema, typeMapToDest: typeMapToDest, destSkipWithNoSchema: destSkipWithNoSchema, srcConverter: srcConverter, srcSchema: srcSchema, typeMapToSrc: typeMapToSrc, srcSkipWithNoSchema: srcSkipWithNoSchema}:
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
