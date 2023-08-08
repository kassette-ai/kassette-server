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
	ID			int				`json:"id"`
	Name		string			`json:"name"`
	ServiceID	int				`json:"service_id"`
	WriteKey	string			`json:"write_key"`
	CustomerID 	int				`json:"customer_id"`
	Config		string			`json:"config"`
	Status		string			`json:"status"`
}

type SourceDetailsT struct {
	Source			SourceInstanceT		`json:"source"`
	Catalogue		ServiceCatalogueT	`json:"catalogue"`
	Destinations	[]string			`json:"destinations"`
}
