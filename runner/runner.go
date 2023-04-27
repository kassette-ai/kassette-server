package runner

import (
	"kassette.ai/kassette-server/routers"
	"log"
)

func CreateServer() {
	endPoint := "8090"
	routersInit := routers.InitRouter()

	err := routersInit.Run(":" + endPoint)

	if err != nil {
		return
	}

	log.Printf("[info] start http server listening %s", endPoint)

}
