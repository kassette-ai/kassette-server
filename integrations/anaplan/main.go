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
	"kassette.ai/kassette-server/integrations"
)

var (
	TypeMapKassetteToDest = map[string]string{
		"NUMBER":   	 	"number",
		"BOOLEAN":   		"bool",
		"TEXT": 	 		"string",
		"DATE":				"date",
	}
	tokenRefreshInterval = 20
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
	AuthUrl				string				`json:"AuthUrl"`
	UserName			string				`json:"UserName"`
	Password			string				`json:"Password"`
	RefreshTokenUrl 	string				`json:"RefreshTokenUrl"`
	Url					string				`json:"Url"`
	Method				string				`json:"Method"`
	Query				string				`json:"Query"`
	Header				map[string]string	`json:"Header"`
	AuthInfo			AuthInfoT
	Client				*http.Client
}

type FailureResponseT struct {
	RequestIndex			int					`json:"requestIndex"`
	FailureType				string				`json:"failureType"`
	FailureMessageDetails	string				`json:"failureMessageDetails"`
}

type ResponseT struct {
	Added			int					`json:"added"`
	Ignored			int					`json:"ignored"`
	Total			int					`json:"total"`
	Failures		[]FailureResponseT	`json:"failures"`
}

func (handle *HandleT) getFullUrl() string {
	if handle.Query == "" {
		return handle.Url
	}
	return handle.Url + "?" + handle.Query
}

func (handle *HandleT) authenticate(isTokenRefresh bool) (bool, string) {
	var req *http.Request
	if isTokenRefresh {
		tokenReq, err := http.NewRequest("POST", handle.RefreshTokenUrl, nil)
		if err != nil {
			return false, err.Error()
		}
		tokenReq.Header.Set("authorization", "AnaplanAuthToken " + handle.AuthInfo.TokenInfo.TokenValue)
		req = tokenReq
	} else {
		authReq, err := http.NewRequest("POST", handle.AuthUrl, nil)
		if err != nil {
			return false, err.Error()
		}
		authReq.SetBasicAuth(handle.UserName, handle.Password)
		req = authReq
	}
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

func (handle *HandleT) refreshAuthToken() (bool, string) {
	ticker := time.NewTicker(time.Duration(tokenRefreshInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <- ticker.C:
			success, _ := handle.authenticate(true)
			if success {
				logger.Info("Anaplan Token Refresh Successful!")
			} else {
				logger.Error("Anaplan Token Refresh Failed!")
			}
		}
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

	refreshTokenUrl, exist := configMap["refreshtokenurl"]
	if !exist {
		logger.Error("RefreshTokenUrl field does not exist in the config data.")
		return false
	}
	handle.RefreshTokenUrl = refreshTokenUrl.(string)

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

	authenticated, errMsg := handle.authenticate(false)

	if authenticated {
		logger.Info(fmt.Sprintf("Rest Destination Connected! %v", *handle))
		go handle.refreshAuthToken()
		return true
	} else {
		logger.Info(fmt.Sprintf("Authentication Failed! %s", errMsg))
		return false
	}
}

func (handle *HandleT) Send(payload json.RawMessage, config map[string]interface{}) (int, string, []interface{}) {
	var failedJobs = []interface{}{}
	var BatchPayloadMapList []integrations.BatchPayloadT
	var idx = 0
	payloads := []map[string]interface{}{}
	json.Unmarshal(payload, &BatchPayloadMapList)
	for _, batchPayload := range BatchPayloadMapList {
		for _, singleEvent := range batchPayload.Payload {
			anaPlanPayload := map[string]interface{}{}
			anaPlanPayload["code"] = fmt.Sprintf("%v-%v", config["JobID"], idx)
			anaPlanPayload["properties"] = singleEvent
			payloads = append(payloads, anaPlanPayload)
			idx ++
		}
	}
	itemsPayload := map[string]interface{}{"items": payloads}
	payloadData, _ := json.Marshal(&itemsPayload)
	logger.Info(fmt.Sprintf("Payload data: %s", payloadData))
	req, err := http.NewRequest(handle.Method, handle.getFullUrl(), bytes.NewBuffer(payloadData))
	logger.Info(fmt.Sprintf("Full Url: %s", handle.getFullUrl()))
	if err != nil {
		return 500, err.Error(), failedJobs
	}
	
	for k, v := range handle.Header {
		req.Header.Add(k, v)
	}

	req.Header.Add("Authorization", "Bearer " + handle.AuthInfo.TokenInfo.TokenValue)

	resp, err := handle.Client.Do(req)
	if err != nil {
		logger.Info(fmt.Sprintf("Rest API job has been failed. Error: %s", err.Error()))
		return resp.StatusCode, err.Error(), failedJobs
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return resp.StatusCode, string(body), failedJobs
	} else {
		var ResponseMap ResponseT
		json.Unmarshal(body, &ResponseMap)
		if ResponseMap.Added == 0 {
			errMsgStr, _ := json.Marshal(ResponseMap.Failures)
			return 500, string(errMsgStr), failedJobs
		} else {
			for _, failure := range ResponseMap.Failures {
				failedPayload := payloads[failure.RequestIndex]
				failedJobs = append(failedJobs, failedPayload["properties"])
			}
		}
	}
	defer resp.Body.Close()
	return resp.StatusCode, "", failedJobs
}
