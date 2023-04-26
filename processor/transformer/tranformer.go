package transformer

import (
	"context"
	jsoniter "github.com/json-iterator/go"
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
	Events       []TransformerResponseT
	FailedEvents []TransformerResponseT
}

type TransformerEventT struct {
	//Message     types.SingularEventT       `json:"message"`
	Metadata MetadataT `json:"metadata"`
	//Destination backendconfig.DestinationT `json:"destination"`
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
