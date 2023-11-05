package camunda

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type CamundaSourceConfig struct {
	Url                  string `json:"url"`
	Count                string `json:"count"`
	Interval             string `json:"interval"`
	History              string `json:"history"`
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

func camundaHistoryRest(url string, api string, batchSize int, fromTime string, toTime string) {

	camundaRestApi := url + "/history/" + api
	queryParams := map[string]string{
		"sortOrder":     "asc",
		"sortBy":        "startTime",
		"startedAfter":  fromTime,
		"startedBefore": toTime,
	}

	// Create a map for headers
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	// Build the URL with query parameters
	req, err := http.NewRequest("GET", camundaRestApi, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	q := req.URL.Query()
	for key, value := range queryParams {
		q.Add(key, value)
	}

	log.Printf("Making Get request to %v ", camundaRestApi)
	req.URL.RawQuery = q.Encode()

	// Add headers to the request
	for key, value := range headers {
		req.Header.Add(key, value)
	}

	// Make the HTTP GET request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Request failed with status: %s\n", resp.Status)
		return
	}

	// Process the response
	log.Printf("Response Status: %v and data: %v", resp.Status, resp.Body)
}

func ExtractCamundaRest(config string, t time.Time) ([]byte, error) {
	var camundaConfig CamundaSourceConfig
	err := json.Unmarshal([]byte(config), &camundaConfig)
	if err != nil {
		log.Printf("Error in the Source config: %v", err)
		return nil, errors.New("Failed to parse Camunda SRC config")
	}

	intervalInt, err := strconv.Atoi(camundaConfig.Interval)
	if err != nil {
		log.Printf("Error: failed to convert interval into number")
		return nil, errors.New("failed to convert interval into number")
	}

	historyInt, err := strconv.Atoi(camundaConfig.History)
	if err != nil {
		log.Printf("Error: failed to convert history into number")
		return nil, errors.New("failed to convert interval into number")
	}

	history := time.Duration(historyInt*60) * time.Second
	duration := time.Duration(intervalInt*60) * time.Second

	from := t.Add(-history).Truncate(time.Minute).Format("2006-01-02T15:04:05.999-0700")
	to := t.Add(-history).Add(duration).Truncate(time.Minute).Format("2006-01-02T15:04:05.999-0700")

	log.Printf("Current time: %v Need to query time from: %v to: %v", t, from, to)
	log.Printf("Config parsed: %v", camundaConfig)
	log.Printf("Polling activityinstance data")
	camundaHistoryRest(camundaConfig.Url, "activity-instance", 100, from, to)

	if camundaConfig.ActivityInstance == "true" {
		log.Printf("Polling activityinstance data")
		camundaHistoryRest(camundaConfig.Url, "activity-instance", 100, from, to)
	}
	return nil, nil

}
