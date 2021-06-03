package rds

import (
	"database/sql"
	"fmt"
	"strconv"

	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer"
	"hisense.com/fusion/dataparser"
	log "hisense.com/fusion/logger"

	"github.com/jmoiron/sqlx"
	"github.com/siddontang/go-mysql/canal"
)

func opSlave(cfg *config.RDSSlaveConfig, sql string) error {
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", cfg.User, cfg.Password, cfg.Addr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("new connection to %s failed, err= %v ", connCfg, err)
		return err
	}
	defer conn.Close()
	_, err = conn.Exec(sql)
	if err != nil {
		log.Logger.Errorf("%s Failed, err = %v", sql, err)
		return err
	}
	return nil
}

func getSlaveStatus(cfg *config.RDSSlaveConfig) (string, uint32, error) {
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", cfg.User, cfg.Password, cfg.Addr)
	conn, err := sql.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("new connection to %s failed, err= %v ", connCfg, err)
		return "", 0, err
	}
	defer conn.Close()
	query, err := conn.Query("show slave status")
	if err != nil {
		log.Logger.Errorf("show slave status Failed, err = %v", err)
		return "", 0, err
	}
	results, err := getResult(query)
	if err != nil {
		log.Logger.Errorf("perse slave result failed, err= %v ", err)
		return "", 0, err
	}
	pos, err := strconv.Atoi(results["Exec_Master_Log_Pos"])
	return results["Master_Log_File"], uint32(pos), nil
}

func getResult(query *sql.Rows) (map[string]string, error) {
	column, _ := query.Columns()              //读出查询出的列字段名
	values := make([][]byte, len(column))     //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column)) //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度
	for i := range values {                   //让每一行数据都填充到[][]byte里面
		scans[i] = &values[i]
	}
	results := make(map[int]map[string]string) //最后得到的map
	i := 0
	for query.Next() { //循环，让游标往下移动
		if err := query.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			fmt.Println(err)
			return nil, err
		}
		row := make(map[string]string) //每行数据
		for k, v := range values {     //每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = string(v)
		}
		results[i] = row //装入结果集中
		i++
	}
	fmt.Println(results)
	for k, v := range results { //查询出来的数组
		fmt.Println(k, v)
	}
	fmt.Println(results[0]["Master_Log_File"])
	fmt.Println(results[0]["Exec_Master_Log_Pos"])
	return results[0], nil
}

func DumpTables(cfg *config.FusionConfig) error {
	canalCfg := *(cfg.CanalConfig)
	canalCfg.Addr = cfg.RDSSlaveConfig.Addr
	canalCfg.User = cfg.RDSSlaveConfig.User
	canalCfg.Password = cfg.RDSSlaveConfig.Password

	c, err := canal.NewCanal(&canalCfg)
	if err != nil {
		log.Logger.Errorf("during dump processing, new canal failed,  err = %v ", err)
		return err
	}
	defer c.Close()

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&dataparser.MyEventHandler{})
	err = c.RunDumpOnly()
	if err != nil {
		log.Logger.Errorf("run dump only failed, err = %v ", err)
		return err
	}
	return nil
}

// RDSSync 如果监听的是aws的rds，则需要从只读副本全量dump数据
func RDSSync(cfg *config.FusionConfig, isManual bool) (string, uint32, error) {
	if len(cfg.RDSSlaveConfig.Addr) == 0 {
		log.Logger.Infof("RDS addr is null, need not full dump from read-only slave")
		return "", 0, nil
	}

	if len(cfg.CanalConfig.Dump.DBTBSMap) == 0 {
		log.Logger.Infof("Dump.DBTBSMap is null, need not full dump from read-only slave")
		return "", 0, nil

	}

	log.Logger.Infof("RDSSync sid=%d", cfg.RuleConfig.SID)
	if !isManual {
		_, binlog, pos := consumer.GetPos(cfg.RuleConfig.SID)
		if len(binlog) != 0 && pos != 0 {
			log.Logger.Infof("working_info has sync pos, need not full dump again")
			return binlog, pos, nil
		}
	}

	// stop slave
	stopSlaveCmd := "call mysql.rds_stop_replication"
	if err := opSlave(cfg.RDSSlaveConfig, stopSlaveCmd); err != nil {
		log.Logger.Errorf("RDS read-only slave exec stop slave cmd err")
		return "", 0, err
	}

	// 获取同步到的master的位置
	masterFile, masterPos, err := getSlaveStatus(cfg.RDSSlaveConfig)
	if err != nil {
		log.Logger.Errorf("RDS read-only slave exec show slave status cmd err")
		return "", 0, err
	}

	// 开启全量dump表数据
	if err := DumpTables(cfg); err != nil {
		log.Logger.Errorf("RDS read-only slave dump tables err")
		return "", 0, err
	}

	// start salve
	startSlaveCmd := "call mysql.rds_start_replication"
	if err := opSlave(cfg.RDSSlaveConfig, startSlaveCmd); err != nil {
		log.Logger.Errorf("RDS read-only slave exec start slave cmd err")
		return "", 0, err
	}
	return masterFile, masterPos, nil
}
