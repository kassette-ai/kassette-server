package backendconfig

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"kassette.ai/kassette-server/utils/logger"
	"os"
	"testing"
)

const incomingPayload = `{
  "config": {
    "a": {
      "aa": {
        "aaa": {
          "sta": "special_object_A",
          "stb": "special_object_B"
        },
        "aab": "bab"
      },
      "ab": true
    },
    "b": "special_string"
  },
  "other": "other"
}`

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

var (
	cb *HandleT = &HandleT{}
)

func shutdown() {

	logger.Info("Cleaning all data from source_config table")
	//cb.dbHandle.Exec("DELETE FROM source_config")
	cb.dbHandle.Close()

}

func setup() {

	viper.SetConfigFile("../config.yaml")
	viper.SetConfigType("yaml")
	// Load configuration from environment variables
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		logger.Error(err.Error())
	}
	cb.Setup()
	logger.Info("initializing DB")
}

func TestSourceSave(t *testing.T) {

	var testSource = SourceT{
		ID:   "1",
		Name: "Camunda-test",
		SourceDefinition: SourceDefinitionT{
			ID:       "1",
			Name:     "Camunda",
			Category: "Workflow",
		},
		WriteKey: "write_key",
		Config:   map[string]interface{}{},
		Enabled:  true,
		Destinations: []DestinationT{
			{
				ID:      "1",
				Name:    "Camunda-test",
				Enabled: true,
				DestinationDefinition: DestinationDefinitionT{
					ID:          "1",
					Name:        "power_bi",
					DisplayName: "Power BI",
					Config: map[string]interface{}{
						"endpoint":      "https://api.powerbi.com/beta/c4ae8b92-c69e-4f24-a16b-9a034ffa7e79/datasets/1c250cc6-b561-4ba2-bcbf-e0c19c7177ee/rows?experience=power-bi&key=yWZBciGHfQkbbTrv4joIJZ3NMFDPClJ0JpQXX4Hul0SbyjKS455l6a2zKhgRF7fLcgszB0enmkANATj%2B1FSFGw%3D%3D",
						"requestMethod": "POST",
						"requestFormat": "ARRAY"},
					ResponseRules: nil,
				},

				Transformations: TransformationT{
					VersionID: "1",
					ID:        "1",
					Config: map[string]interface{}{
						"originalTimestamp":    "Time",
						"camunda_process_name": "Data",
					},
				},
			},
		},
	}

	cb.insertSource("write_key", testSource)

	assert.True(t, true, "True is true!")

}

//var _ = BeforeSuite(func() {
//	var err error
//	psqlInfo := jobsdb.GetConnectionString()
//	dbHandle, err = sql.Open("postgres", psqlInfo)
//	if err != nil {
//		panic(err)
//	}
//	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
//	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
//})

func TestTransform(t *testing.T) {

	fmt.Print("asdasds")
	//
	//err := json.Unmarshal([]byte(myJSON), &biggerType)
	//if err != nil {
	//	panic(err)
	//}

}

func TestSavingConfig(t *testing.T) {

}
