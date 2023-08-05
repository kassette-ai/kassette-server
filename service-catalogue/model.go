package service_catalogue

type ServiceCatalogue struct {
	ID 			string			`json:"id"`
	Name		string			`json:"name"`
	Type		string			`json:"type"`
	Access		string			`json:"access"`
	Category	string			`json:"category"`
	Url			string			`json:"url"`
	Notes		string			`json:"notes"`
	MetaData	string			`json:"metadata"`
	Icon 		string			`json:"icon"`
}
