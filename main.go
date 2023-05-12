package main

import (
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"kassette.ai/kassette-server/gateway"
	jobsdb "kassette.ai/kassette-server/jobs"
	"log"
)

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

	// source environment variables
	//setupPostgres()
	var gatewayDB jobsdb.HandleT
	gatewayDB.Setup(false, "gw", 0, false)

	var gateway gateway.HandleT
	gateway.Setup(&gatewayDB)

}

