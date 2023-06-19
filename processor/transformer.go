package processor

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/utils/logger"
	"sort"
	"sync"
)

type MetadataT struct {
	SourceID            string                            `json:"sourceId"`
	WorkspaceID         string                            `json:"workspaceId"`
	Namespace           string                            `json:"namespace"`
	InstanceID          string                            `json:"instanceId"`
	SourceType          string                            `json:"sourceType"`
	SourceCategory      string                            `json:"sourceCategory"`
	TrackingPlanId      string                            `json:"trackingPlanId"`
	TrackingPlanVersion int                               `json:"trackingPlanVersion"`
	SourceTpConfig      map[string]map[string]interface{} `json:"sourceTpConfig"`
	MergedTpConfig      map[string]interface{}            `json:"mergedTpConfig"`
	DestinationID       string                            `json:"destinationId"`
	JobID               int64                             `json:"jobId"`
	SourceJobID         string                            `json:"sourceJobId"`
	SourceJobRunID      string                            `json:"sourceJobRunId"`
	SourceTaskRunID     string                            `json:"sourceTaskRunId"`
	RecordID            interface{}                       `json:"recordId"`
	DestinationType     string                            `json:"destinationType"`
	MessageID           string                            `json:"messageId"`
	OAuthAccessToken    string                            `json:"oauthAccessToken"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds"`
	KassetteID              string   `json:"kassetteId"`
	ReceivedAt              string   `json:"receivedAt"`
	EventName               string   `json:"eventName"`
	EventType               string   `json:"eventType"`
	SourceDefinitionID      string   `json:"sourceDefinitionId"`
	DestinationDefinitionID string   `json:"destinationDefinitionId"`
	TransformationID        string   `json:"transformationId"`
	TransformationVersionID string   `json:"transformationVersionId"`
	SourceDefinitionType    string   `json:"-"`
}

const (
	UserTransformerStage        = "user_transformer"
	EventFilterStage            = "event_filter"
	DestTransformerStage        = "dest_transformer"
	TrackingPlanValidationStage = "trackingPlan_validation"
)

const (
	StatusCPDown              = 809
	TransformerRequestFailure = 909
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type Transformer interface {
	Setup()
	Transform(ctx context.Context, clientEvents []TransformerEventT, url string, batchSize int) ResponseT
	Validate(clientEvents []TransformerEventT, url string, batchSize int) ResponseT
}

type ResponseT struct {
	Events       []interface{}
	Success      bool
	SourceIDList []string
}

type TransformerEventT struct {
	//Message     types.SingularEventT       `json:"message"`
	Metadata    MetadataT                  `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
	//Libraries   []backendconfig.LibraryT   `json:"libraries"`
}

type TransformerResponseT struct {
	Output           map[string]interface{} `json:"output"`
	Metadata         MetadataT              `json:"metadata"`
	StatusCode       int                    `json:"statusCode"`
	Error            string                 `json:"error"`
	ValidationErrors []ValidationErrorT     `json:"validationErrors"`
}

type ValidationErrorT struct {
	Type    string            `json:"type"`
	Message string            `json:"message"`
	Meta    map[string]string `json:"meta"`
}

type transformerHandleT struct {
	requestQ   chan *transformMessageT
	responseQ  chan *transformMessageT
	accessLock sync.Mutex
	//perfStats    *misc.PerfStats
	//sentStat     *stats.KassetteStats
	//receivedStat *stats.KassetteStats
	//failedStat   *stats.KassetteStats
}

type transformMessageT struct {
	index int
	data  interface{}
	url   string
}

func (trans *transformerHandleT) transformWorker() {

	//batching payload
	for job := range trans.requestQ {

		reqArray := job.data.([]interface{})
		respArray := transformBatchPayload(reqArray)

		trans.responseQ <- &transformMessageT{data: respArray, index: job.index}
		logger.Info("10")
	}
}

// transformToPayload function is used to transform the message to payload
func transformToPayload(m map[string]interface{}) map[string]interface{} {

	rawTransform := make(map[string]interface{})

	rawPayload := m["message"].(map[string]interface{})
	// This is where the transformation happens
	destination := m["destination"].(backendconfig.DestinationT)

	transformationsMap := destination.Transformations.Config

	transformedPayload := make(map[string]interface{})

	for k, _ := range rawPayload {
		if _, ok := transformationsMap[k]; ok {
			switch val := transformationsMap[k].(type) {
			case bool:
				if val {
					transformedPayload[k] = rawPayload[k]
				}
			case string:
				//If it's a string we rename the key
				transformedPayload[val] = rawPayload[k]
			}
		} else {
			transformedPayload[k] = rawPayload[k]
		}
	}

	// Converting to array to support PowerBi

	rawTransform["payload"] = [1]interface{}{transformedPayload}
	rawTransform["endpoint"] = destination.DestinationDefinition.Config["endpoint"]
	rawTransform["userId"] = "userId"
	rawTransform["header"] = map[string]string{"Content-Type": "application/json"}
	rawTransform["requestConfig"] = destination.DestinationDefinition.Config

	logger.Debug(fmt.Sprintf("Transformed payload: %v", rawTransform))

	return rawTransform
}

func transformBatchPayload(m []interface{}) map[string]interface{} {

	rawTransform := make(map[string]interface{})

	// This is where the transformation happens
	sample := m[0]
	destination := sample.(map[string]interface{})["destination"].(backendconfig.DestinationT)

	transformationsMap := destination.Transformations.Config

	batchPayload := make([]interface{}, 0)

	for _, rawMap := range m {

		rawMap := rawMap.(map[string]interface{})
		rawPayload := rawMap["message"].(map[string]interface{})
		transformedPayload := make(map[string]interface{})

		for k, _ := range rawPayload {

			if _, ok := transformationsMap[k]; ok {
				switch val := transformationsMap[k].(type) {
				case bool:
					if val {
						transformedPayload[k] = rawPayload[k]
					}
				case string:
					//If it's a string we rename the key
					transformedPayload[val] = rawPayload[k]
				}
			} else {
				transformedPayload[k] = rawPayload[k]
			}

		}
		batchPayload = append(batchPayload, transformedPayload)
	}

	// Converting to array to support PowerBi
	rawTransform["payload"] = batchPayload
	rawTransform["endpoint"] = destination.DestinationDefinition.Config["endpoint"]
	rawTransform["userId"] = "userId"
	rawTransform["header"] = map[string]string{"Content-Type": "application/json"}
	rawTransform["requestConfig"] = destination.DestinationDefinition.Config

	logger.Debug(fmt.Sprintf("Transformed payload: %v", rawTransform))

	return rawTransform
}

// Transform function is used to invoke transformer API
// Transformer is not thread safe. So we need to create a new instance for each request
func (trans *transformerHandleT) Transform(clientEvents []interface{},
	url string, batchSize int) ResponseT {

	logger.Info("Transform!!!!")

	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()

	var transformResponse = make([]*transformMessageT, 0)
	//Enqueue all the jobs
	inputIdx := 0
	outputIdx := 0
	totalSent := 0
	reqQ := trans.requestQ
	resQ := trans.responseQ

	var toSendData interface{}
	var sourceIDList []string
	for _, clientEvent := range clientEvents {
		sourceIDList = append(sourceIDList, clientEvent.(map[string]interface{})["source_id"].(string))
	}

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

		logger.Info("8")

		select {
		//In case of batch event, index is the next Index
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, url: url}:
			totalSent++
			toSendData = nil
			if inputIdx == len(clientEvents) {
				reqQ = nil
			}
			logger.Info("11")
		case data := <-resQ:
			transformResponse = append(transformResponse, data)
			outputIdx++
			//If all was sent and all was received we are done
			if reqQ == nil && outputIdx == totalSent {
				resQ = nil
			}
			logger.Info("12")
		}
		if reqQ == nil && reqQ != nil && len(reqQ) == 0 {
			break
		}

		if reqQ == nil && resQ == nil {
			logger.Info("13")
			break
		}
	}

	logger.Info("14")

	logger.Info(fmt.Sprintf("Sort events"))

	//Sort the responses in the same order as input
	sort.Slice(transformResponse, func(i, j int) bool {
		return transformResponse[i].index < transformResponse[j].index
	})

	outClientEvents := make([]interface{}, 0)
	var outClientEventsSourceIDs []string

	for idx, resp := range transformResponse {
		if resp.data == nil {
			continue
		}

		outClientEvents = append(outClientEvents, resp.data)
		outClientEventsSourceIDs = append(outClientEventsSourceIDs, sourceIDList[idx])

	}

	return ResponseT{
		Events:       outClientEvents,
		Success:      true,
		SourceIDList: outClientEventsSourceIDs,
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
