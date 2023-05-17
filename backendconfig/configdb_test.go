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
	cb.dbHandle.Exec("DELETE FROM source_config")

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
		Config:  map[string]interface{}{},
		Enabled: true,
		Destinations: []DestinationT{
			{
				ID:   "1",
				Name: "Camunda-test",
				DestinationDefinition: DestinationDefinitionT{
					ID:            "1",
					Name:          "power_bi",
					DisplayName:   "Power BI",
					Config:        map[string]interface{}{},
					ResponseRules: nil,
				},
			},
		},
	}

	cb.createSource("write_key", testSource)

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
