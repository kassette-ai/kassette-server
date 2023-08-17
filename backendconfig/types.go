package backendconfig

import (
	"database/sql"
)

type SourceT struct {
	ID               string                 `json:"id"`
	Name             string                 `json:"sourceName"`
	SourceDefinition SourceDefinitionT      `json:"sourceDefinition"`
	Config           map[string]interface{} `json:"config"`
	Enabled          bool                   `json:"enabled"`
	Destinations     []DestinationT         `json:"destinations"`
	WriteKey         string                 `json:"writeKey"`
}

type DgSourceTrackingPlanConfigT struct {
	SourceId            string                            `json:"sourceId"`
	SourceConfigVersion int                               `json:"version"`
	Config              map[string]map[string]interface{} `json:"config"`
	MergedConfig        map[string]interface{}            `json:"mergedConfig"`
	Deleted             bool                              `json:"deleted"`
	TrackingPlan        TrackingPlanT                     `json:"trackingPlan"`
}

type TrackingPlanT struct {
	Id      string `json:"id"`
	Version int    `json:"version"`
}

type SourceDefinitionT struct {
	ID       string `json:"sourceId"`
	Name     string `json:"sourceName"`
	Category string `json:"category"`
}

type DestinationDefinitionT struct {
	ID            string
	Name          string
	DisplayName   string
	Config        map[string]interface{}
	ResponseRules map[string]interface{}
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                map[string]interface{}
	Enabled               bool
	WorkspaceID           string
	Transformations       TransformationT
	IsProcessorEnabled    bool
	RevisionID            string
}

type TransformationT struct {
	VersionID string
	ID        string
	Config    map[string]interface{}
}

/*---- Type definitions for the new structure ----*/

var SourceStatus, DestinationStatus	map[string]string

func init() {
	SourceStatus = map[string]string{
		"ENABLED": "enabled",
		"DISABLED": "disabled",
	}
	DestinationStatus = map[string]string{
		"ENABLED": "enabled",
		"DISABLED": "disabled",
	}
}

func (source SourceInstanceT) Enabled() bool {
	return source.Status == SourceStatus["ENABLED"]
}

func (source SourceInstanceT) Disabled() bool {
	return source.Status == SourceStatus["DISABLED"]
}

func (dest DestinationInstanceT) Enabled() bool {
	return dest.Status == DestinationStatus["ENABLED"]
}

func (dest DestinationInstanceT) Disabled() bool {
	return dest.Status == DestinationStatus["DISABLED"]
}

type HandleT struct {
	dbHandle        *sql.DB
	destinationList []DestinationT
}

type ServiceCatalogueT struct {
	ID 			int				`json:"id"`
	Name		string			`json:"name"`
	Type		string			`json:"type"`
	Access		string			`json:"access"`
	Category	string			`json:"category"`
	Url			string			`json:"url"`
	Notes		string			`json:"notes"`
	MetaData	string			`json:"metadata"`
	IconUrl 	string			`json:"iconurl"`
}

type SourceInstanceT struct {
	ID				int				`json:"id"`
	Name			string			`json:"name"`
	ServiceID		int				`json:"service_id"`
	WriteKey		string			`json:"write_key"`
	CustomerID 		int				`json:"customer_id"`
	CustomerName	string			`json:"customer_name"`
	SecretKey		string			`json:"secret_key"`
	Config			string			`json:"config"`
	Status			string			`json:"status"`
}

type DestinationInstanceT struct {
	ID			int				`json:"id"`
	Name		string			`json:"name"`
	ServiceID	int				`json:"service_id"`
	CustomerID 	int				`json:"customer_id"`
	Config		string			`json:"config"`
	Status		string			`json:"status"`
}

type SourceDetailT struct {
	Source			SourceInstanceT		`json:"source"`
	Catalogue		ServiceCatalogueT	`json:"catalogue"`
}

type DestinationDetailT struct {
	Destination		DestinationInstanceT	`json:"destination"`
	Catalogue		ServiceCatalogueT		`json:"catalogue"`
}

type SourceConnectionsT struct {
	SourceDetail			SourceDetailT				`json:"source_detail"`
	DestinationDetails		[]DestinationDetailT		`json:"destination_details"`
}

type DestinationConnectionsT struct {
	DestinationDetail		DestinationDetailT			`json:"destination_detail"`
	SourceDetails			[]SourceDetailT				`json:"source_details`
}

type ConnectionInstanceT struct {
	ID				int				`json:"id"`
	SourceID		int				`json:"source_id"`
	DestinationID	int				`json:"destination_id"`
	Transforms		string			`json:"transforms"`
}

type ConnectionDetailT struct {
	Connection			ConnectionInstanceT	`json:"connection"`
	SourceDetail		SourceDetailT		`json:"source_detail"`
	DestinationDetail	DestinationDetailT	`json:"destination_detail"`
}

type ConnectionDetailsT struct {
	Connections []ConnectionDetailT `json:"connections"`
}
