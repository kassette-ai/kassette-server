package backendconfig

type SourceT struct {
	ID               string                 `json:"id"`
	Name             string                 `json:"sourceName"`
	SourceDefinition SourceDefinitionT      `json:"sourceDefinition"`
	Config           map[string]interface{} `json:"config"`
	Enabled          bool                   `json:"enabled"`
	Destinations     []DestinationT         `json:"destinations"`
	WriteKey         string                 `json:"writeKey"`
}

type TableConfig struct {
	Agent  string              `json:"kassette_data_agent"`
	Type   string              `json:"kassette_data_type"`
	Config []map[string]string `json:"config"`
}

type SourceAdvancedConfig struct {
	Source []TableConfig `json:"source_config"`
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
