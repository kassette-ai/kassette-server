package routers

import (
	"github.com/gin-gonic/gin"
)

func InitRouter() *gin.Engine {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.POST("/collect", collect)

	return r
}
func collect(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "hello world",
	})
}
