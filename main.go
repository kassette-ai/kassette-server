package main

import (
	"github.com/bugsnag/bugsnag-go"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/processor"
	"kassette.ai/kassette-server/router"
	"kassette.ai/kassette-server/utils/logger"
	"log"
	"time"
)

var (
	gwDBRetention     time.Duration
	routerDBRetention time.Duration

	enabledDestinations []backendconfig.DestinationT

	rawDataDestinations []string
)

func loadConfig() {
	gwDBRetention = viper.GetDuration("gwDBRetention") * time.Hour
	routerDBRetention = viper.GetDuration("routerDBRetention")
}

func main() {
	viper.SetConfigFile("config.yaml")
	viper.SetConfigType("yaml")

	// Load configuration from environment variables
	viper.AutomaticEnv()
	err := viper.ReadInConfig()

	if err != nil {
		log.Println(err)
		return
	}

	logger.Info("Starting Kassette Server")
	bugsnag.Configure(bugsnag.Configuration{
		// Your BugSnag project API key, required unless set as environment
		// variable $BUGSNAG_API_KEY
		APIKey: viper.GetString("bugsnag.apiKey"),
		// The development stage of your application build, like "alpha" or
		// "production"
		ReleaseStage: viper.GetString("bugsnag.stage"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "kassette.ai/kassette-server"},
		// more configuration options
	})

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var configDB backendconfig.HandleT
	var gateway gateway.HandleT
	var processor processor.HandleT
	var router router.HandleT

	configDB.Init()

	gatewayDB.Setup(false, "gw", 0, false, true)
	routerDB.Setup(false, "rt", routerDBRetention, false, true)
	batchRouterDB.Setup(false, "batch_rt", routerDBRetention, false, true)

	processor.Setup(&gatewayDB, &routerDB, &batchRouterDB)
	router.Setup(&routerDB, &configDB)
	gateway.Setup(&gatewayDB, &configDB)
}
