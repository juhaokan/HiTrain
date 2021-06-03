package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"hisense.com/fusion/config"
	"hisense.com/fusion/dump"
	"hisense.com/fusion/healthcheck"
)

// https://www.jianshu.com/p/5973d1999f5d
// healthCheck if mysql or redis is not ok ,return check failed TEST: curl -X GET http://10.18.210.132:12345/fusion/health
func healthCheck(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var errRedis error
	errMysql := healthcheck.CheckTargetMysqlHealth()
	if config.ConfigInfo.KafkaConfig.NeedKafka == 0 || config.ConfigInfo.KafkaConfig.NeedKafka == 2 {
		errRedis = healthcheck.CheckTargetRedisHealth()
	}
	if errMysql != nil || errRedis != nil {
		msg := fmt.Sprintf("errMysqlMsg:%v , errRedisMsg:%v", errMysql, errRedis)
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write([]byte(msg))
	} else {
		resp.WriteHeader(http.StatusOK)
		resp.Write([]byte("FusionDataProcessing Health Check Success"))
	}
}

// sync data to redis manually TEST:curl -X POST -d  '{"tables":"hello.hello1,"}' http://10.18.210.132:12345/fusion/datasync
func syncDataToRedis(w http.ResponseWriter, r *http.Request) {
	var req map[string]interface{}
	body, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(body, &req)
	//sid := req["sid"].(string)
	tbs := req["tables"].(string)
	go dump.Manually(tbs, 0)
	// composite response body
	var res = map[string]string{"result": "success", "tables": tbs}
	response, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(response)
}
