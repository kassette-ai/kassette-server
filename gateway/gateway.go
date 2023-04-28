package gateway

import (
	"github.com/spf13/viper"
	"time"
)

func loadConfig() {

	//Port where GW is running
	webPort = viper.GetInt("Gateway.webPort")
	//Number of incoming requests that are batched before initiating write
	maxBatchSize = config.GetInt("Gateway.maxBatchSize", 32)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	batchTimeout = (config.GetDuration("Gateway.batchTimeoutInMS", time.Duration(20)) * time.Millisecond)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 4)
	// CustomVal is used as a key in the jobsDB customval column
	CustomVal = config.GetString("Gateway.CustomVal", "GW")
	//Reponse message sent to client
	respMessage = config.GetString("Gateway.respMessage", "OK")
	// Maximum request size to gateway
	maxReqSize = config.GetInt("Gateway.maxReqSizeInKB", 100000) * 1000
}
