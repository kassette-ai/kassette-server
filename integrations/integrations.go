package integrations

import (
	"fmt"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/misc"
	"strings"
)

var (
	destTransformURL, userTransformURL string
)

func GetDestinationIDs(clientEvent interface{}, destNameIDMap map[string]backendconfig.DestinationDefinitionT) (retVal []string) {
	clientIntgs, ok := misc.GetKassetteEventVal("integrations", clientEvent)
	if !ok {
		return
	}

	clientIntgsList, ok := clientIntgs.(map[string]interface{})
	if !ok {
		return
	}
	var outVal []string
	for dest := range destNameIDMap {
		if clientIntgsList[dest] == false {
			continue
		}
		if (clientIntgsList["All"] != false) || clientIntgsList[dest] == true {
			outVal = append(outVal, destNameIDMap[dest].Name)
		}
	}
	retVal = outVal
	return
}

// GetDestinationURL returns node URL
func GetDestinationURL(destID string) string {
	return fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destID))
}
