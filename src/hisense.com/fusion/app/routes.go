package app

import (
	"github.com/gorilla/mux"
)

//NewRouter 创建路由
func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	router.
		HandleFunc("/fusion/health", healthCheck).
		Methods("GET")
	router.
		HandleFunc("/fusion/datasync", syncDataToRedis).
		Methods("POST")
	return router
}
