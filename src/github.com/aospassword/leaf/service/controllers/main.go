package main

import (
	"errors"
	"github.com/aospassword/leaf/core/common"
	"github.com/gin-gonic/gin"
)

func main()  {
	router := gin.Default()
	router.GET("/api/segment/get/:key")
	router.GET("/api/snowflake/get/:key")
}

func get(key string,id common.Result) (string,error) {

	if len(key) == 0 {
		return "",errors.New("key is none")
	}
	result := id

	if	result.GetStatus() == common.Err {
		return "",errors.New(result.String())
	}

	return string(result.GetID()), nil
}