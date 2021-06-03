package healthcheck

import (
	"fmt"
	"strings"
	"time"

	"hisense.com/fusion/config"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/redisutil"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// FailoverChan 通过该通道通知main函数，重启canal 解析
var FailoverChan = make(chan int)

// Variables mysql 变量信息， 例如read_only 值得查询
type Variables struct {
	VariableName  string `db:"VARIABLE_NAME"`
	VariableValue string `db:"VARIABLE_VALUE"`
}

// CheckTargetMysqlHealth 检查Mysql 健康状态 for jucloud
func CheckTargetMysqlHealth() error {
	//cfg := config.ConfigInfo.CanalConfig
	user, password, addr := selectTargetMysql()
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", user, password, addr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("health check connect to mysql %s failed, err= %v ", connCfg, err)
		return err
	}
	defer conn.Close()
	heartbeatSQL := config.ConfigInfo.AppConfig.Heartbeat
	if len(heartbeatSQL) == 0 {
		heartbeatSQL = "select user()"
	}
	outInfo := []interface{}{}
	err = conn.Select(&outInfo, heartbeatSQL)
	if err != nil {
		log.Logger.Errorf("health check connect to mysql Failed, sql = %s, err = %v", heartbeatSQL, err)
		if IsMaxConnERR(err) {
			log.Logger.Warnf("check target status failed, Too many connections! ignore err!")
			return nil
		}
		return err
	}
	return nil
}

func connectOnMaster() (*sqlx.DB, error) {
	user := config.ConfigInfo.CanalConfig.User
	password := config.ConfigInfo.CanalConfig.Password
	addr := config.ConfigInfo.CanalConfig.Addr
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", user, password, addr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("health check connect to mysql %s failed, err= %v ", connCfg, err)
		return conn, err
	}
	log.Logger.Debugf("ready to check mysql status , connCfg= %s", connCfg)
	return conn, nil
}

func connectOnStandby() (*sqlx.DB, error) {
	user := config.ConfigInfo.StandbyConfig.User
	password := config.ConfigInfo.StandbyConfig.Password
	addr := config.ConfigInfo.StandbyConfig.Addr
	if len(addr) == 0 {
		log.Logger.Errorf("standby config error, addr is nil")
		return nil, fmt.Errorf("standby addr is nil")
	}
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", user, password, addr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("health check connect to mysql %s failed, err= %v ", connCfg, err)
		return conn, err
	}
	log.Logger.Debugf("ready to check mysql status , connCfg= %s", connCfg)
	return conn, nil
}

// checkTargetStatusMatch read_only = OFF
func checkTargetStatusMatch(conn *sqlx.DB) bool {
	var out Variables
	var sql string
	mysqlVersion := strings.Split(config.ConfigInfo.AppConfig.MysqlVersion, ".")[0]
	log.Logger.Debugf("mysqlVersion = %s", mysqlVersion)
	if mysqlVersion == "8" {
		sql = "select * from performance_schema.global_variables where variable_name='read_only'"
	} else {
		sql = "select * from information_schema.global_variables where variable_name='read_only'"
	}

	err := conn.Get(&out, sql)
	if err != nil {
		log.Logger.Errorf("check target status failed, err = %v", err)
		if IsMaxConnERR(err) {
			log.Logger.Warnf("check target status failed, Too many connections! ignore err!")
			return true
		}
		return false
	}
	log.Logger.Debugf("check target status , get read_only = %s", out.VariableValue)
	if out.VariableValue == "OFF" {
		return true
	}
	if out.VariableValue == "ON" {

		return false
	}
	return true
}

// selectTargetMysql 返回连接的mysql 地址 0. 可连接， 1. show slave status  为空， 2， read_only = OFF
func selectTargetMysql() (string, string, string) {
	for {
		masterReady := false
		backupReady := false
		conn, err := connectOnMaster()
		if err == nil {
			// 如果是监听单点的slave，不做readonly检查
			if config.ConfigInfo.AppConfig.ISSyncFromSlave == 1 {
				log.Logger.Infof("fusion sync from slave, not check readonly, is ready ......")
				masterReady = true
			} else {
				if checkTargetStatusMatch(conn) {
					log.Logger.Debugf("master is ready ......")
					masterReady = true
				}
			}
			conn.Close()
		}
		conn, err = connectOnStandby()
		if err == nil {
			if checkTargetStatusMatch(conn) {
				log.Logger.Debugf("backup is ready ......")
				backupReady = true
			}
			conn.Close()
		}
		// 两个节点都是ready 的需要继续检测， 只能有一个节点是ready 的fusion 才开始工作，避免出现连接到从库
		if masterReady && backupReady {
			log.Logger.Warnf("master mysql and backup mysql are both ready, continue to check")
			time.Sleep(time.Second)
			continue
		}
		if masterReady && !backupReady {
			log.Logger.Infof("master mysql is ready !!!, addr = %s", config.ConfigInfo.CanalConfig.Addr)
			return config.ConfigInfo.CanalConfig.User, config.ConfigInfo.CanalConfig.Password, config.ConfigInfo.CanalConfig.Addr
		}
		if !masterReady && backupReady {
			log.Logger.Infof("bakcup mysql is ready !!!, addr = %s", config.ConfigInfo.StandbyConfig.Addr)
			return config.ConfigInfo.StandbyConfig.User, config.ConfigInfo.StandbyConfig.Password, config.ConfigInfo.StandbyConfig.Addr
		}
		log.Logger.Warnf("master ready state  is %v, backup ready state is %v", masterReady, backupReady)
		time.Sleep(time.Second)
	}

}

// GetTargetMysql 获取fusion 要连接的mysql 信息， 目前只通过read_only 来判断是否符合要求
func GetTargetMysql() (string, string, string) {
	user, password, addr := selectTargetMysql()
	config.TargetMysqlAddr = addr
	return user, password, addr
}

// MonitorTargetMysqlHealth 检查是否出现mysql 主从切换
func MonitorTargetMysqlHealth() error {
	failoverCount := 0
	var user string
	var password string
	var addr string
	user, password, addr = selectTargetMysql()
	failoverEnable := 1
	failoverMAXCheck := config.ConfigInfo.AppConfig.FailoverMAXCheck
	if failoverMAXCheck == 0 {
		failoverMAXCheck = 60
	}
	heartbeatSQL := config.ConfigInfo.AppConfig.Heartbeat
	if len(heartbeatSQL) == 0 {
		heartbeatSQL = "select user()"
	}
	for {
		time.Sleep(time.Second)
		connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", user, password, addr)
		conn, err := sqlx.Open("mysql", connCfg)
		if err != nil {
			log.Logger.Errorf("health check connect to mysql %s failed, failoverCount = %d, err= %v ", connCfg, failoverCount, err)
			conn.Close()
			failoverCount = failoverCount + 1
			if failoverCount > failoverMAXCheck {
				user, password, addr = selectTargetMysql()
				failoverCount = 0
				FailoverChan <- failoverEnable
				time.Sleep(3 * time.Second)
				user, password, addr = selectTargetMysql()
			}
			continue
		}

		outInfo := []interface{}{}
		err = conn.Select(&outInfo, heartbeatSQL)
		if err != nil {
			log.Logger.Errorf("health check exec sql Failed, failoverCount = %d, sql = %s, err = %v", failoverCount, heartbeatSQL, err)
			conn.Close()

			if IsMaxConnERR(err) {
				log.Logger.Warnf("health check exec sql Failed, Too many connections! ignore err!")
			} else {
				failoverCount = failoverCount + 1
			}

			if failoverCount > failoverMAXCheck {
				user, password, addr = selectTargetMysql()
				failoverCount = 0
				FailoverChan <- failoverEnable
				time.Sleep(3 * time.Second)
				user, password, addr = selectTargetMysql()
			}
			continue
		}
		failoverCount = 0
		conn.Close()
	}
}

// IsMaxConnERR 判断是否是mysql连接数打满的错误码 1040: Too many connections
func IsMaxConnERR(err error) bool {
	if driverErr, ok := err.(*mysql.MySQLError); ok {
		if driverErr.Number == 1040 {
			return true
		}
	}
	return false

}

// CheckTargetRedisHealth 检查Redis 健康状态
func CheckTargetRedisHealth() error {
	conn := redisutil.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("ping")
	if err != nil {
		log.Logger.Errorf("health check ping redis error, err = %v", err)
		return err
	}
	return nil
}

// GetMasterAddr 获取fusion 要连接的mysql addr
func GetMasterAddr() string {
	_, _, addr := selectTargetMysql()
	return addr
}
