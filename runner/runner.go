package runner

import (
	"fmt"
	"kassette.ai/kassette-server/routers"
	"log"
	"net/http"
)

func CreateServer() {
	endPoint := "8090"
	routersInit := routers.InitRouter()

	server := &http.Server{
		Addr:           ":" + endPoint,
		Handler:        routersInit,
		ReadTimeout:    60,
		WriteTimeout:   60,
		MaxHeaderBytes: 1 << 20,
	}

	err := server.ListenAndServe()
	if err != nil {
		fmt.Println(fmt.Sprintf("Error: %s", err))
		return
	}
	log.Printf("[info] start http server listening %s", endPoint)

}
