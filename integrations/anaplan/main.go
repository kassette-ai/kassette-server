package anaplan

import (
	"io"
	"fmt"
	"time"
	"bytes"
	"encoding/json"
	"net/url"
	"net/http"
	"kassette.ai/kassette-server/utils/logger"
)

type MetaT struct {
	ValidationUrl			string			`json:"validationUrl"`
}

type TokenInfoT struct {
	ExpiresAt				int64			`json:"expiresAt"`
	TokenID					string			`json:"tokenId"`
	TokenValue				string			`json:"tokenValue"`
	RefreshTokenId			string			`json:"refreshTokenId"`
}

type AuthInfoT struct {
	Meta				MetaT				`json:"meta"`
	Status				string				`json:"status"`
	StatusMessage		string				`json:"statusMessage"`
	TokenInfo			TokenInfoT			`json:"tokenInfo"`
}

type HandleT struct {
	AuthUrl			string				`json:"AuthUrl"`
	UserName		string				`json:"UserName"`
	Password		string				`json:"Password"`
	Url				string				`json:"Url"`
	Method			string				`json:"Method"`
	Query			string				`json:"Query"`
	Header			map[string]string	`json:"Header"`
	AuthInfo		AuthInfoT
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

func (handle *HandleT) authenticate() (bool, string) {
	req, err := http.NewRequest("POST", handle.AuthUrl, nil)
	if err != nil {
		return false, err.Error()
	}
	req.SetBasicAuth(handle.UserName, handle.Password)

	resp, err := handle.Client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err.Error()
	}
	err = json.Unmarshal(body, &handle.AuthInfo)
	if err != nil {
		return false, err.Error()
	}
	if handle.AuthInfo.Status == "SUCCESS" {
		return true, ""
	} else {
		return false, handle.AuthInfo.StatusMessage
	}
}

func (handle *HandleT) Init(config string) bool {
	var configMap map[string]interface{}
	err := json.Unmarshal([]byte(config), &configMap)
	if err != nil {
		logger.Error(fmt.Sprintf("Error occurred while Unmarshaling Rest Config Data. Error: %s", err.Error()))
		return false
	}

	authUrl, exist := configMap["authurl"]
	if !exist {
		logger.Error("AuthUrl field does not exist in the config data.")
		return false
	}
	handle.AuthUrl = authUrl.(string)

	userName, exist := configMap["username"]
	if !exist {
		logger.Error("UserName field does not exist in the config data.")
		return false
	}
	handle.UserName = userName.(string)

	password, exist := configMap["password"]
	if !exist {
		logger.Error("Password field does not exist in the config data.")
		return false
	}
	handle.Password = password.(string)

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

	authenticated, errMsg := handle.authenticate()

	if authenticated {
		logger.Info(fmt.Sprintf("Rest Destination Connected! %v", *handle))
		return true
	} else {
		logger.Info(fmt.Sprintf("Authentication Failed! %s", errMsg))
		return false
	}
}

func (handle *HandleT) Send(payload json.RawMessage) (int, json.RawMessage) {
	currentTime := time.Now()
	currentTimeStr := currentTime.Format("1970-01-01 00:00:00")
	var BatchPayloadMapList []BatchPayloadT
	payloads := []map[string]interface{}{}
	json.Unmarshal(payload, &BatchPayloadMapList)
	for _, batchPayload := range BatchPayloadMapList {
		for idx, singleEvent := range batchPayload.Payload {
			anaPlanPayload := map[string]interface{}{}
			anaPlanPayload["code"] = fmt.Sprintf("%s-%d", currentTimeStr, idx)
			anaPlanPayload["properties"] = singleEvent
			payloads = append(payloads, anaPlanPayload)
		}
	}
	itemsPayload := map[string]interface{}{"items": payloads}
	payloadData, _ := json.Marshal(&itemsPayload)
	logger.Info(fmt.Sprintf("Payload data: %s", payloadData))
	req, err := http.NewRequest(handle.Method, handle.getFullUrl(), bytes.NewBuffer(payloadData))
	logger.Info(fmt.Sprintf("Full Url: %s", handle.getFullUrl()))
	if err != nil {
		return 500, []byte(fmt.Sprintf(`{"error": %s}`, err.Error()))
	}
	
	for k, v := range handle.Header {
		req.Header.Add(k, v)
	}

	req.Header.Add("Authorization", "Bearer " + handle.AuthInfo.TokenInfo.TokenValue)

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
