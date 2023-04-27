package routers

import (
	"github.com/gin-gonic/gin"
)

func InitRouter() *gin.Engine {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.GET("/", hello)

	return r
}
func hello(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "hello world",
	})
}
