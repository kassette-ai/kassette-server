package camunda

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

type ActivityInstance struct {
	Id                       string `json:"id"`
	ParentActivityInstanceId string `json:"parentActivityInstanceId"`
	ActivityId               string `json:"activityId"`
	ActivityName             string `json:"activityName"`
	ActivityType             string `json:"activityType"`
	ProcessDefinitionKey     string `json:"processDefinitionKey"`
	ProcessDefinitionId      string `json:"processDefinitionId"`
	ProcessInstanceId        string `json:"processInstanceId"`
	ExecutionId              string `json:"executionId"`
	TaskId                   string `json:"taskId"`
	CalledProcessInstanceId  string `json:"calledProcessInstanceId"`
	CalledCaseInstanceId     string `json:"calledCaseInstanceId"`
	Assignee                 string `json:"assignee"`
	StartTime                string `json:"startTime"`
	EndTime                  string `json:"endTime"`
	DurationInMillis         int    `json:"durationInMillis"`
	Canceled                 bool   `json:"canceled"`
	CompleteScope            bool   `json:"completeScope"`
	TenantId                 string `json:"tenantId"`
	RemovalTime              string `json:"removalTime"`
	RootProcessInstanceId    string `json:"rootProcessInstanceId"`
	KassetteType             string `json:"kassetteType"`
}

type ProcessInstance struct {
	Id                     string `json:"id"`
	SuperProcessInstanceId string `json:"superProcessInstanceId"`
	SuperCaseInstanceId    string `json:"superCaseInstanceId"`
	CaseInstanceId         string `json:"caseInstanceId"`
	ProcessDefinitionKey   string `json:"processDefinitionKey"`
	ProcessDefinitionId    string `json:"processDefinitionId"`
	BusinessKey            string `json:"businessKey"`
	StartTime              string `json:"startTime"`
	EndTime                string `json:"endTime"`
	DurationInMillis       int    `json:"durationInMillis"`
	StartUserId            string `json:"startUserId"`
	StartActivityId        string `json:"startActivityId"`
	DeleteReason           string `json:"deleteReason"`
	TenantId               string `json:"tenantId"`
	KassetteType           string `json:"kassetteType"`
}

func camundaHistoryRest(url string, api string, batchSize int, fromTime string, toTime string) ([]byte, error) {

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
		return nil, err
	}

	q := req.URL.Query()
	for key, value := range queryParams {
		q.Add(key, value)
	}

	log.Printf("Making Get request to %v with parameters %v", camundaRestApi, queryParams)
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
		return nil, err
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Request failed with status: %s\n", resp.Status)
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed reading Body response %v", err)
		return nil, err
	}

	return body, nil

}

func ExtractCamundaRest(config string, t time.Time) ([][]byte, error) {
	var camundaConfig CamundaSourceConfig
	var combinedCamundaPayload [][]byte
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

	from := t.Add(-history).Truncate(time.Minute).Format("2006-01-02T15:04:05.000-0700")
	to := t.Add(-history).Add(duration).Truncate(time.Minute).Format("2006-01-02T15:04:05.000-0700")

	if camundaConfig.ActivityInstance == "true" {
		log.Printf("Polling activityinstance data")
		var payload []ActivityInstance
		activityInstancePayload, err := camundaHistoryRest(camundaConfig.Url, "activity-instance", 100, from, to)
		if err != nil {
			log.Printf("Failed extracting activiti-instance data: %v", err)
			return nil, err
		}

		errj := json.Unmarshal(activityInstancePayload, &payload)
		if errj != nil {
			log.Printf("Failed parsing response Body %v", errj)
			return nil, errj
		}

		//updating metadata for every event
		for i := range payload {
			payload[i].KassetteType = "activity-instance"
		}

		payloadBatch := make(map[string][]ActivityInstance)
		payloadBatch["batch"] = payload

		jsonData, err := json.Marshal(payloadBatch)
		if err != nil {
			log.Printf("Can't convert into JSON: %v", err)
			return nil, err
		}
		if len(payload) > 0 {
			combinedCamundaPayload = append(combinedCamundaPayload, jsonData)
		}

	}

	if camundaConfig.ProcessInstace == "true" {
		log.Printf("Polling processinstance data")
		var payload []ProcessInstance
		processInstancePayload, err := camundaHistoryRest(camundaConfig.Url, "process-instance", 100, from, to)
		if err != nil {
			log.Printf("Failed extracting process-instance data: %v", err)
			return nil, err
		}

		errj := json.Unmarshal(processInstancePayload, &payload)
		if errj != nil {
			log.Printf("Failed parsing response Body %v", errj)
			return nil, errj
		}

		//updating metadata for every event
		for i := range payload {
			payload[i].KassetteType = "process-instance"
		}

		payloadBatch := make(map[string][]ProcessInstance)
		payloadBatch["batch"] = payload

		jsonData, err := json.Marshal(payloadBatch)
		if err != nil {
			log.Printf("Can't convert into JSON: %v", err)
			return nil, err
		}

		if len(payload) > 0 {
			combinedCamundaPayload = append(combinedCamundaPayload, jsonData)
		}

	}

	return combinedCamundaPayload, nil
}
