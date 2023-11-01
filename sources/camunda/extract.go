package camunda

import (
	"encoding/json"
	"log"
	"time"
)

type CamundaSourceConfig struct {
	Url                  string `json:"url"`
	Task                 string `json:"task"`
	Batch                string `json:"batch"`
	Detail               string `json:"detail"`
	Schema               string `json:"schema"`
	JobLog               string `json:"job-log"`
	Incident             string `json:"incident"`
	CaseInstance         string `json:"case-instance"`
	UaerOperation        string `json:"user-operation"`
	ProcessInstace       string `json:"process-instance"`
	ActivityInstance     string `json:"activity-instance"`
	DecisionInstance     string `json:"decision-instance"`
	IdentityLinkLog      string `json:"identity-link-log"`
	VariableInstance     string `json:"variable-instance"`
	ProcessDefinition    string `json:"process-definition"`
	CaseActivityInstance string `json:"case-activity-instance"`
}

func camundaHistoryRest(url string, api string, currentTime time.Time, lastId string) {

	//camundaRequest := url + "/history/" + api
	// queryParams := map[string]string{
	// 	"sortOrder": "asc",
	// 	"sortBy": "startTime",
	// 	"maxResults": batchSize
	// }
	// req, err := http.NewRequest("GET", url, bytes.NewBuffer(jsonData))

}

func ExtractCamundaRest(configString string) {
	var config CamundaSourceConfig
	err := json.Unmarshal([]byte(configString), &config)
	if err != nil {
		log.Print("Failed to parse Camunda SRC config")
		return
	}
	if config.ActivityInstance == "true" {
		//here call rest api method for camunda-api /history/activity-instance
	}
	log.Printf("Url: %s", config.Url)
}
