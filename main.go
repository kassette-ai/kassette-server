package main

import (
	"log"
	"time"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/processor"
	"kassette.ai/kassette-server/router"
	"kassette.ai/kassette-server/utils/logger"
	"github.com/spf13/viper"
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

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var configDB backendconfig.HandleT
	var gateway gateway.HandleT
	var processor processor.HandleT
	var router router.HandleT

	configDB.Init()
	gatewayDB.Setup(false, "gw", 0, false, false)
	routerDB.Setup(false, "rt", routerDBRetention, false, true)
	batchRouterDB.Setup(false, "batch_rt", routerDBRetention, false, false)

	processor.Setup(&gatewayDB, &routerDB, &batchRouterDB)
	router.Setup(&routerDB, &configDB)
	gateway.Setup(&gatewayDB, &configDB)
}
