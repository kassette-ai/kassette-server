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

	for job := range trans.requestQ {

		// Just sending the data back for now....
		respArray := job.data.([]interface{})

		for _, respElem := range respArray {
			respElemMap := transformToPayload(respElem.(map[string]interface{}))
			trans.responseQ <- &transformMessageT{data: respElemMap, index: job.index}
		}
	}

}

// transformToPayload function is used to transform the message to payload
func transformToPayload(m map[string]interface{}) map[string]interface{} {

	rawTransform := make(map[string]interface{})
	rawTransform["payload"] = m["message"]
	rawTransform["endpoint"] = "https://api.powerbi.com/beta/c4ae8b92-c69e-4f24-a16b-9a034ffa7e79/datasets/1c250cc6-b561-4ba2-bcbf-e0c19c7177ee/rows?experience=power-bi&key=yWZBciGHfQkbbTrv4joIJZ3NMFDPClJ0JpQXX4Hul0SbyjKS455l6a2zKhgRF7fLcgszB0enmkANATj%2B1FSFGw%3D%3D"
	rawTransform["userId"] = "userId"
	rawTransform["header"] = map[string]string{"Content-Type": "application/json"}
	rawTransform["requestConfig"] = "config"

	logger.Debug(fmt.Sprintf("Transformed payload: %v", rawTransform))

	return rawTransform
}

// Transform function is used to invoke transformer API
// Transformer is not thread safe. So we need to create a new instance for each request
func (trans *transformerHandleT) Transform(clientEvents []interface{},
	url string, batchSize int) ResponseT {

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
	sourceIDList := []string{}
	for _, clientEvent := range clientEvents {
		sourceIDList = append(sourceIDList, clientEvent.(map[string]interface{})["message"].(map[string]interface{})["source_id"].(string))
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

		select {
		//In case of batch event, index is the next Index
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, url: url}:
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
		if reqQ == nil && resQ == nil {
			break
		}
	}

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
