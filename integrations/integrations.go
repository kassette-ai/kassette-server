package integrations

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/misc"
	"strings"
)

var (
	destTransformURL, userTransformURL string
)

type PostParameterT struct {
	URL           string
	Type          int
	UserID        string
	Payload       interface{}
	Header        interface{}
	RequestConfig map[string]interface{}
}

func GetPostInfo(transformRaw json.RawMessage) PostParameterT {
	var postInfo PostParameterT
	var ok bool
	parsedJSON := gjson.ParseBytes(transformRaw)
	postInfo.URL, ok = parsedJSON.Get("endpoint").Value().(string)
	misc.Assert(ok)
	postInfo.UserID, ok = parsedJSON.Get("userId").Value().(string)
	misc.Assert(ok)
	postInfo.Payload, ok = parsedJSON.Get("payload").Value().(interface{})
	misc.Assert(ok)
	postInfo.Header, ok = parsedJSON.Get("header").Value().(interface{})
	misc.Assert(ok)
	postInfo.RequestConfig, ok = parsedJSON.Get("requestConfig").Value().(map[string]interface{})
	misc.Assert(ok)
	return postInfo
}

func GetDestinationIDs(clientEvent interface{}, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	//clientIntgs, ok := misc.GetKassetteEventVal("integrations", clientEvent)
	//if clientIntgs == nil || !ok {
	//	return
	//}
	//
	//clientIntgsList, ok := clientIntgs.(map[string]interface{})
	//if !ok {
	//	return
	//}
	var outVal []string
	for dest := range destNameIDMap {
		//if clientIntgsList[dest] == false {
		//	continue
		//}
		//if (clientIntgsList["All"] != false) || clientIntgsList[dest] == true {
		outVal = append(outVal, destNameIDMap[dest].Name)
		//}
	}
	retVal = outVal
	return
}

// GetDestinationURL returns node URL
func GetDestinationURL(destID string) string {
	return fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destID))
}

const (
	//PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	//PostDataJSON means post data is sent as JSON
	PostDataJSON
	//PostDataXML means post data is sent as XML
	PostDataXML

	PostDataArray // Post data is sent as an array of JSON objects
)
