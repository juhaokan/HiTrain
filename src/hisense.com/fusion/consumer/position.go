package consumer

import (
	"fmt"
	"time"

	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"

	"github.com/jmoiron/sqlx"
)

var posDict = []utility.WorkingInfo{}

func getConnection() (*sqlx.DB, error) {
	cfg := config.ConfigInfo.RuleConfig
	connCfg := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Addr, cfg.Port, cfg.Schema)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		fmt.Printf("err = %v\n", err)
		log.Logger.Errorf("new connection to %s failed, err= %v ", connCfg, err)
		return nil, err
	}
	return conn, nil
}

// GetPos 获取解析到的位置点
func GetPos(sid int) (string, string, uint32) {
	var info []utility.WorkingInfo
	conn, err := getConnection()
	if err != nil {
		return "", "errpos", 0
	}
	defer conn.Close()
	err = conn.Select(&info, "select * from working_info where sid=?", config.SID)
	if err != nil {
		log.Logger.Errorf("get pos info info = %v failed", err)
		return "", "errpos", 0
	}
	if len(info) == 0 {
		//return config.TargetMysqlAddr, "", 0
		return "", "", 0
	}
	return info[0].MysqlAddr, info[0].FileName, info[0].Position

}

// ClearPos 删除同步的位置点， 在mysql 发生failover 时使用
func ClearPos(sid int) bool {
	conn, err := getConnection()
	if err != nil {
		return false
	}
	defer conn.Close()
	_, err = conn.Exec("delete from working_info where sid=?", config.SID)
	if err != nil {
		log.Logger.Errorf("clear pos failed, err = %v", err)
		return false
	}
	log.Logger.Infof("clear pos success ,sid = %d", config.SID)
	return true
}

func getPositionInfo(data utility.WorkingInfo) (string, uint32, uint32) {
	return data.FileName, data.Position, data.Timestamp
}

// SavePos 异步将解析到的位置点写到数据库中 
func SavePos(sid int) {
	conn, err := getConnection()
	if err != nil {
		return
	}
	stmt, err := conn.Prepare("replace into working_info value(?,?,?,?,?)")
	if err != nil {
		log.Logger.Errorf("prepare sql failed")
		return
	}
	var lastSavePoint int64
	saveInterval := config.ConfigInfo.AppConfig.SaveInterval
	for {
		select {
		case data := <-utility.PosChan:
			binlogFile, pos, timestamp := getPositionInfo(data)
			if saveInterval <= 0 {
				_, err = stmt.Exec(config.SID, config.TargetMysqlAddr, binlogFile, pos, timestamp)
			} else {
				current := time.Now().UnixNano()
				if (current - lastSavePoint) > saveInterval*1e6 {
					_, err = stmt.Exec(config.SID, config.TargetMysqlAddr, binlogFile, pos, timestamp)
					lastSavePoint = current
				}
			}
			if err != nil {
				log.Logger.Errorf("save pos failed, err= %v", err)
			}
			log.Logger.Infof("parser mysql binlog positon to file = %s, pos = %d", binlogFile, pos)
		}
	}
}
