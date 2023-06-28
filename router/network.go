package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"kassette.ai/kassette-server/integrations"
	"kassette.ai/kassette-server/misc"
)

func (network *NetHandleT) sendPost(jsonData []byte) (int, string, string) {

	client := network.httpClient

	//Parse the response to get parameters
	postInfo := integrations.GetPostInfo(jsonData)

	requestMethod, ok := postInfo.RequestConfig["requestMethod"].(string)
	misc.Assert(ok && (requestMethod == "POST" || requestMethod == "GET"))
	requestFormat := postInfo.RequestConfig["requestFormat"].(string)
	misc.Assert(ok)

	switch requestFormat {
	case "PARAMS":
		postInfo.Type = integrations.PostDataKV
	case "JSON":
		postInfo.Type = integrations.PostDataJSON
	case "ARRAY":
		postInfo.Type = integrations.PostDataArray
	default:
		misc.Assert(false)
	}

	var req *http.Request
	var err error
	if useTestSink {
		req, err = http.NewRequest(requestMethod, testSinkURL, nil)
		misc.AssertError(err)
	} else {
		req, err = http.NewRequest(requestMethod, postInfo.URL, nil)
		misc.AssertError(err)
	}

	queryParams := req.URL.Query()
	if postInfo.Type == integrations.PostDataKV {
		payloadKV, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		for key, val := range payloadKV {
			queryParams.Add(key, fmt.Sprint(val))
		}
	} else if postInfo.Type == integrations.PostDataJSON {
		payloadJSON, ok := postInfo.Payload.(map[string]interface{})
		misc.Assert(ok)
		jsonValue, err := json.Marshal(payloadJSON)
		misc.AssertError(err)
		req.Body = io.NopCloser(bytes.NewReader(jsonValue))
	} else if postInfo.Type == integrations.PostDataArray {
		payloadArray := postInfo.Payload
		misc.Assert(ok)
		jsonValue, err := json.Marshal(payloadArray)
		misc.AssertError(err)
		req.Body = io.NopCloser(bytes.NewReader(jsonValue))
	} else {
		//Not implemented yet
		misc.Assert(false)
	}

	req.URL.RawQuery = queryParams.Encode()

	headerKV, ok := postInfo.Header.(map[string]interface{})
	misc.Assert(ok)
	for key, val := range headerKV {
		req.Header.Add(key, val.(string))
	}

	req.Header.Add("User-Agent", "Kassette")

	resp, err := client.Do(req)

	var respBody []byte

	if resp != nil && resp.Body != nil {
		respBody, _ = io.ReadAll(resp.Body)
		defer resp.Body.Close()
	}

	if err != nil {
		log.Println("Errored when sending request to the server", err)
		return http.StatusGatewayTimeout, "", string(respBody)
	}

	return resp.StatusCode, resp.Status, string(respBody)
}

// Setup initializes the module
func (network *NetHandleT) Setup(destID string) {
	log.Println("Network Handler Startup")

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	misc.Assert(ok)
	var defaultTransportCopy http.Transport
	misc.Copy(&defaultTransportCopy, defaultTransportPointer)
	defaultTransportCopy.MaxIdleConns = 100
	defaultTransportCopy.MaxIdleConnsPerHost = 100
	network.httpClient = &http.Client{Transport: &defaultTransportCopy}

}
