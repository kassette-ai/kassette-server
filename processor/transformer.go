package processor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"strconv"
	"encoding/json"
	"kassette.ai/kassette-server/utils/logger"
)

var (
	TransType map[string]string
	SystemTransformationRules []TransformationRuleT
)

func init() {
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
}

type Transformer interface {
	Setup()
	Transform(ctx context.Context, clientEvents []interface{}, ruleStr string, batchSize int) ResponseT
}

type ResponseT struct {
	Events       []interface{}
	Success      bool
}

type TransformationRuleT struct {
	From		string		`json:"from"`
	To			string		`json:"to"`
	Field		string		`json:"field"`
	Type		string		`json:"type"`
	Value		string		`json:"value"`
}

type transformerHandleT struct {
	requestQ   chan *transformMessageT
	responseQ  chan *transformMessageT
	accessLock sync.Mutex
}

type transformMessageT struct {
	index int
	data  interface{}
	rules  []TransformationRuleT
}

func (trans *transformerHandleT) transformWorker() {

	//batching payload
	for job := range trans.requestQ {
		reqArray := job.data.([]interface{})
		respArray := transformBatchPayload(reqArray, job.rules)
		trans.responseQ <- &transformMessageT{data: respArray, index: job.index}
	}
}

func transformBatchPayload(m []interface{}, rules []TransformationRuleT) map[string]interface{} {

	rawTransform := make(map[string]interface{})
	batchPayload := make([]interface{}, 0)

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
			if !hide {
				transformedPayload[fieldName] = rawPayload[k]
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
func (trans *transformerHandleT) Transform(clientEvents []interface{}, ruleStr string, batchSize int) ResponseT {

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
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, rules: rules}:
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
