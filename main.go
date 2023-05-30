package main

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/backendconfig"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"kassette.ai/kassette-server/processor"
	"kassette.ai/kassette-server/router"
	"kassette.ai/kassette-server/utils"
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

	// source environment variables
	//setupPostgres()
	var gatewayDB jobsdb.HandleT
	gatewayDB.Setup(false, "gw", 0, false)

	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT

	var configDB backendconfig.HandleT
	configDB.Init()

	routerDB.Setup(false, "rt", routerDBRetention, false)
	batchRouterDB.Setup(false, "batch_rt", routerDBRetention, false)

	var processor processor.HandleT
	processor.Setup(&gatewayDB, &routerDB, &batchRouterDB)

	go monitorDestRouters(&routerDB, &batchRouterDB)

	var gateway gateway.HandleT
	gateway.Setup(&gatewayDB, &configDB)

}

func monitorDestRouters(routerDB, batchRouterDB *jobsdb.HandleT) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch)
	dstToRouter := make(map[string]*router.HandleT)

	for {
		config := <-ch

		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations = enabledDestinations[:0]
		for _, source := range sources.Sources {
			if source.Enabled {
				for _, destination := range source.Destinations {
					if destination.Enabled {
						enabledDestinations = append(enabledDestinations, destination)

						rt, ok := dstToRouter[destination.DestinationDefinition.Name]
						if !ok {
							logger.Info(fmt.Sprintf("Starting a new Destination", destination.DestinationDefinition.Name))
							var router router.HandleT
							router.Setup(routerDB, destination.DestinationDefinition.Name)
							dstToRouter[destination.DestinationDefinition.Name] = &router
						} else {
							rt.Enable()
						}

					}
				}
			}
		}
		for destID, rtHandle := range dstToRouter {
			found := false
			for _, dst := range enabledDestinations {
				if destID == dst.DestinationDefinition.Name {
					found = true
					break
				}
			}
			//Router is not in enabled list. Disable it
			if !found {
				rtHandle.Disable()
			}
		}

	}
}
