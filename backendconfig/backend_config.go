package backendconfig

import (
	"kassette.ai/kassette-server/utils"
	"time"
)

var (
	configBackendURL, configBackendToken string
	pollInterval                         time.Duration
	curSourceJSON                        SourcesT
	initialized                          bool
)

var (
	Eb *utils.EventBus
)

type SourcesT struct {
	Sources []SourceT `json:"sources"`
}

func loadConfig() {

	pollInterval = 5 * time.Second
}

func getBackendConfig() (sources []SourcesT, destination []DestinationT) {

	//if resp != nil && resp.Body != nil {
	//	respBody, _ = io.ReadAll(resp.Body)
	//	defer resp.Body.Close()
	//}
	//if err != nil {
	//	Logger.Error(fmt.Sprint("Errored when sending request to the server", err))
	//	return SourcesT{}, false
	//}
	//var sourcesJSON SourcesT
	//err = json.Unmarshal(respBody, &sourcesJSON)
	//if err != nil {
	//	log.Println("Errored while parsing request", err, string(respBody), resp.StatusCode)
	//	return SourcesT{}, false
	//}
	return
}

func init() {

	loadConfig()
}

func pollConfigUpdate() {
	//for {
	//	sourceJSON, ok := getBackendConfig()
	//
	//
	//	time.Sleep(time.Duration(pollInterval))
	//}
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
