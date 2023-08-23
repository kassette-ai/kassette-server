package rest

import (
	"io"
	"fmt"
	"bytes"
	"encoding/json"
	"net/url"
	"net/http"
	"kassette.ai/kassette-server/utils/logger"
)

type HandleT struct {
	Url				string				`json:"Url"`
	Method			string				`json:"Method"`
	Query			string				`json:"Query"`
	Header			map[string]string	`json:"Header"`
	Client			*http.Client
}

type BatchPayloadT struct {
	Payload			[]interface{}		`json:"payload"`
}

func (handle *HandleT) getFullUrl() string {
	if handle.Query == "" {
		return handle.Url
	}
	return handle.Url + "?" + handle.Query
}

func (handle *HandleT) Init(config string) bool {
	var configMap map[string]interface{}
	err := json.Unmarshal([]byte(config), &configMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred while Unmarshaling Rest Config Data. Error: %s", err.Error()))
		return false
	}

	destUrl, exist := configMap["url"]
	if !exist {
		logger.Error("Url field does not exist in the config data.")
		return false
	}
	handle.Url = destUrl.(string)

	method, exist := configMap["method"]
	if !exist {
		logger.Error("Method field does not exist in the config data.")
		return false
	}
	handle.Method = method.(string)
	
	query, exist := configMap["query"]
	if !exist {
		logger.Error("Query field does not exist in the config data.")
		return false
	}
	
	var queryMap map[string]string
	err = json.Unmarshal([]byte(query.(string)), &queryMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred while Unmarshaling Rest Config Query Data. Error: %s", err.Error()))
		return false
	}

	queryParams := url.Values{}
	for k, v := range queryMap {
		queryParams.Add(k, v)
		logger.Info(fmt.Sprintf("Query Params: %s %s", k, v))
	}
	handle.Query = queryParams.Encode()

	header, exist := configMap["header"]
	if !exist {
		logger.Error("Header field does not exist in the config data.")
		return false
	}
	err = json.Unmarshal([]byte(header.(string)), &handle.Header)
	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred while Unmarshaling Rest Config Header Data. Error: %s", err.Error()))
		return false
	}

	handle.Client = &http.Client{}

	logger.Info(fmt.Sprintf("Rest Destination Connected! %v", *handle))
	
	return true
}

func (handle *HandleT) Send(payload json.RawMessage) (int, json.RawMessage) {
	var BatchPayloadMapList []BatchPayloadT
	payloads := []interface{}{}
	json.Unmarshal(payload, &BatchPayloadMapList)
	for _, batchPayload := range BatchPayloadMapList {
		payloads = append(payloads, batchPayload.Payload...)
	}
	payloadData, _ := json.Marshal(&payloads)
	logger.Info(fmt.Sprintf("Payload data: %s", payloadData))
	req, err := http.NewRequest(handle.Method, handle.getFullUrl(), bytes.NewBuffer(payloadData))
	logger.Info(fmt.Sprintf("Full Url: %s", handle.getFullUrl()))
	if err != nil {
		return 500, []byte(fmt.Sprintf(`{"error": %s}`, err.Error()))
	}
	
	for k, v := range handle.Header {
		req.Header.Add(k, v)
	}

	resp, err := handle.Client.Do(req)
	if err != nil {
		logger.Info(fmt.Sprintf("Rest API job has been failed. Error: %s", err.Error()))
		return resp.StatusCode, []byte(fmt.Sprintf(`{"error: "%s"}`, err.Error()))
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, body
	}
	defer resp.Body.Close()
	return resp.StatusCode, []byte(`{"status": "ok"}`)
}
