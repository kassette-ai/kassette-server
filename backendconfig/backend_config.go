package backendconfig

import (
	"io"
	. "kassette.ai/kassette-server/utils"
	"log"
)

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"time"
)

var (
	configBackendURL, configBackendToken string
	pollInterval                         time.Duration
	curSourceJSON                        SourcesT
	initialized                          bool
)

var Eb *EventBus

type SourcesT struct {
	Sources []SourceT `json:"sources"`
}

func loadConfig() {
	configBackendURL = ""
	configBackendToken = ""
	pollInterval = 5 * time.Second
}

func getBackendConfig() (SourcesT, bool) {
	client := &http.Client{}
	url := fmt.Sprintf("%s/workspace-config?workspaceToken=%s", configBackendURL, configBackendToken)
	resp, err := client.Get(url)

	var respBody []byte
	if resp != nil && resp.Body != nil {
		respBody, _ = io.ReadAll(resp.Body)
		defer resp.Body.Close()
	}
	if err != nil {
		Logger.Error(fmt.Sprintf("Errored when sending request to the server", err))
		return SourcesT{}, false
	}
	var sourcesJSON SourcesT
	err = json.Unmarshal(respBody, &sourcesJSON)
	if err != nil {
		log.Println("Errored while parsing request", err, string(respBody), resp.StatusCode)
		return SourcesT{}, false
	}
	return sourcesJSON, true
}

func init() {

	loadConfig()
}

func pollConfigUpdate() {
	for {
		sourceJSON, ok := getBackendConfig()
		if !ok {
			log.Println("Errored when getting config to the server")
		}
		if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
			curSourceJSON = sourceJSON
			initialized = true
			Eb.Publish("backendconfig", sourceJSON)
		}
		time.Sleep(time.Duration(pollInterval))
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func Subscribe(channel chan DataEvent) {
	Eb.Subscribe("backendconfig", channel)
	Eb.PublishToChannel(channel, "backendconfig", curSourceJSON)
}

func WaitForConfig() {
	for {
		if initialized {
			break
		}
		time.Sleep(time.Duration(pollInterval))

	}
}

// Setup backend config
func Setup() {
	Eb = new(EventBus)
	go pollConfigUpdate()
}
