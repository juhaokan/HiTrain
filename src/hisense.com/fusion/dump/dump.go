package dump

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"hisense.com/fusion/config"
	"hisense.com/fusion/dataparser"
	"hisense.com/fusion/healthcheck"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/rds"
	"hisense.com/fusion/ruleparser"

	"github.com/jmoiron/sqlx"
	"github.com/siddontang/go-mysql/canal"
	//"github.com/siddontang/go-log/log"
)

// MasterStatus keep master file and pos
type MasterStatus struct {
	File            string `db:"File"`
	Position        int    `db:"Position"`
	BinlogDoDB      string `db:"Binlog_Do_DB"`
	BinlogIgnoreDB  string `db:"Binlog_Ignore_DB"`
	ExecutedGtidSet string `db:"Executed_Gtid_Set"`
}

// MysqlPosInfo mysql_pos_info
type MysqlPosInfo struct {
	SID              int         `db:"sid"`
	MasterMysqlAddr  string      `db:"master_mysql_addr"`
	MasterBinlogFile interface{} `db:"master_binlog_file_name"`
	MasterBinlogPos  interface{} `db:"master_binlog_position"`
	SlaveMysqlAddr   string      `db:"slave_mysql_addr"`
	SlaveBinlogFile  string      `db:"slave_binlog_file_name"`
	SlaveBinlogPos   int         `db:"slave_binlog_position"`
}

// DumpFlagChan dump flag chan
var DumpFlagChan = make(chan int)

// RuleChan 重新启动后，需要将rule operation 字段置成0
var RuleChan = make(chan int)

var (
	// SetDumpFlag 设置dump 标志
	SetDumpFlag = 1
	// ReleaseDumpFlag 释放dump 标志
	ReleaseDumpFlag = 0
	// AddRuleFlag 添加规则标志
	AddRuleFlag = 2
	// DelRuleFlag 删除规则标志
	DelRuleFlag = 3
	// RestartCanalFlag 重启解析标志
	RestartCanalFlag = 4
	// RuleApplySuccess 规则修改完成
	RuleApplySuccess = 1
)

//getDumpsTablesMap tables 的格式应该为 "hello.hello1, hello.hello2, sss.kkk"
func getDumpsTablesMap(tables string) map[string][]string {
	dumpTables := make(map[string][]string)
	arr := strings.Split(tables, ",")
	for _, dbtb := range arr {
		_dbtb := strings.Split(dbtb, ".")
		if len(_dbtb) != 2 {
			log.Logger.Errorf("dump tables has err format, table = %s, should be db.tb format ", dbtb)
			continue
		}
		db := strings.TrimSpace(_dbtb[0])
		tb := strings.TrimSpace(_dbtb[1])
		_, ok := dumpTables[db]
		if !ok {
			dumpTables[db] = []string{tb}
		} else {
			dumpTables[db] = append(dumpTables[db], tb)
		}
	}
	log.Logger.Infof("dump tables = %+v ", dumpTables)
	return dumpTables
}

func releaseDumpFlagChan(flag int) {
	DumpFlagChan <- flag
}

func setDumpFlagChan() {
	DumpFlagChan <- SetDumpFlag
}

// Manually flag = 0 手动dump， 用以恢复不一致的内存数据, flag = 2 , 添加新规则，全量dump 需要dump 的数据, flag = 1 删除规则， 重启fusion 加载最新的同步表信息
func Manually(tables string, flag int) error {
	setDumpFlagChan()
	defer releaseDumpFlagChan(flag)
	if flag == RestartCanalFlag {
		return nil
	}

	dumpTables := getDumpsTablesMap(tables)
	if len(dumpTables) == 0 {
		return fmt.Errorf("no table to dump")
	}
	cfg := config.InitCfgUseFile()
	if cfg == nil {
		log.Logger.Infof("cfg == nil ")
		return fmt.Errorf("during dump processing, get config file failed")
	}
	cfg.CanalConfig.Dump.DBTBSMap = dumpTables

	if len(cfg.RDSSlaveConfig.Addr) != 0 {
		log.Logger.Infof("mysql is RDS, need full dump from read-only slave")
		_, _, err := rds.RDSSync(cfg, true)
		if err != nil {
			log.Logger.Errorf("Manually RDSSync failed, err = %v ", err)
			return err
		}
		return nil
	}
	user, password, addr := healthcheck.GetTargetMysql()
	canCfg := *(cfg.CanalConfig)
	canCfg.Addr = addr
	canCfg.User = user
	canCfg.Password = password
	c, err := canal.NewCanal(&canCfg)
	if err != nil {
		log.Logger.Errorf("during dump processing, new canal failed,  err = %v ", err)
		return err
	}
	defer c.Close()

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&dataparser.MyEventHandler{})
	log.Logger.Debugf("Manually RunDumpOnly begin.")
	err = c.RunDumpOnly()
	if err != nil {
		log.Logger.Errorf("run dump only failed, err = %v ", err)
		return err
	}
	log.Logger.Debugf("Manually RunDumpOnly end.")
	return nil
}

// Monitor 监听表规则是否增加or 删除， 并根据配置信息决定是否dump 数据
func Monitor() {
	cfg := config.InitCfgUseFile()
	toKafka := cfg.KafkaConfig.NeedKafka
	for {
		waitStartFusionFlag := false
		time.Sleep(15 * time.Second)
		restartCanal, dumpTables, delTables := ruleparser.MonitorRuleChange(cfg.RuleConfig, toKafka)
		if len(dumpTables) > 0 {
			log.Logger.Infof("get add rule tables = %s", dumpTables)
			Manually(dumpTables, AddRuleFlag)
			waitStartFusionFlag = true
		} else {
			if restartCanal || len(delTables) > 0 {
				log.Logger.Infof("get del rule tables = %s", delTables)
				Manually("", RestartCanalFlag)
				waitStartFusionFlag = true
			}
		}

		if waitStartFusionFlag {
			select {
			case <-RuleChan:
				for {
					err := ruleparser.ResetOperationColumn(cfg.RuleConfig)
					if err != nil {
						time.Sleep(3 * time.Second)
						continue
					}
					break
				}
			}
		}
		log.Logger.Infof("monitoring table rule change ......")
	}
}

// GetMasterStatus 获取master binlog file and pos
func GetMasterStatus() (string, int) {
	user, password, addr := healthcheck.GetTargetMysql()
	var masterStatus []MasterStatus
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", user, password, addr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("get master status failed,  err= %v ", err)
		return "", 0
	}
	defer conn.Close()

	sql := fmt.Sprintf("show master status")
	err = conn.Select(&masterStatus, sql)
	if err != nil {
		log.Logger.Errorf("get master status failed, sql = %s, err = %v", sql, err)
		return "", 0
	}

	return masterStatus[0].File, masterStatus[0].Position
}

// GetNewMasterBinlogInfo 主从切换后，根据fusion处理到的老主节点的位置信息和mysql_pos_info表中信息来确定新主的位置进行同步
func GetNewMasterBinlogInfo(mysqlAddr string, binlog string, pos uint32) (string, int) {
	switchType := config.ConfigInfo.AppConfig.MysqlSwitchType
	var newBinlog string
	var newPos int
	if switchType == 0 {
		newBinlog, newPos = GetNewBinlogBefore(mysqlAddr, binlog, pos)
		log.Logger.Infof("GetNewMasterBinlogInfo switchType = 0 working_info mysqlAddr=%s, binlog=%s, pos=%d ; get newBinlog=%s, newPos=%d", mysqlAddr, binlog, pos, newBinlog, newPos)
		if newPos == 0 {
			log.Logger.Fatalf("GetNewMasterBinlogInfo GetNewBinlogBefore err, system exiting....")
		}
	} else if switchType == 1 {
		// 若可以丢数据，则从新主当前位置开始同步
		newBinlog, newPos = GetMasterStatus()
		log.Logger.Infof("GetNewMasterBinlogInfo switchType = 1 working_info mysqlAddr=%s, binlog=%s, pos=%d ; get newBinlog=%s, newPos=%d", mysqlAddr, binlog, pos, newBinlog, newPos)
		if newPos == 0 {
			log.Logger.Fatalf("get new master status failed, system exiting....")
		}
	} else {
		log.Logger.Errorf("switchType=%d is not available!", switchType)
	}
	return newBinlog, newPos

}

// GetNewBinlogBefore 新主根据mysql_pos_info表向前最接近working_info位置点，作为同步位置
func GetNewBinlogBefore(mysqlAddr string, binlog string, pos uint32) (string, int) {
	sql := fmt.Sprintf("select * from mysql_pos_info where id=(select max(id) from mysql_pos_info where sid=? and master_mysql_addr=? and ((master_binlog_file_name=? and master_binlog_position<=?) or (master_binlog_file_name<?)))")
	log.Logger.Infof("GetNewBinlogBefore sql: %s, sid=%d, master_mysql_addr=%s, binlog=%s, pos=%d", sql, config.ConfigInfo.RuleConfig.SID, mysqlAddr, binlog, pos)
	newBinlog, newPos := GetBinlogFromMysqlPosInfo(sql, config.ConfigInfo.RuleConfig.SID, mysqlAddr, binlog, pos, binlog)
	if newPos == 0 {
		log.Logger.Errorf("GetNewBinlogBefore failed pos is 0 ")
		return "", 0
	}
	return newBinlog, newPos
}

// GetBinlogFromMysqlPosInfo 查询mysql_pos_info表获取新主对应的位置
func GetBinlogFromMysqlPosInfo(sql string, args ...interface{}) (string, int) {
	conn, err := getConnection()
	if err != nil {
		log.Logger.Errorf("get connection from rule mysql err!")
		return "", 0
	}
	defer conn.Close()

	query, err := conn.Query(sql, args...)
	if err != nil {
		log.Logger.Errorf("GetBinlogFromMysqlPosInfo exec sql= %s failed, value=%v, err= %v ", sql, config.TargetMysqlAddr, err)
		return "", 0
	}

	results, err := getResult(query)
	if err != nil {
		log.Logger.Errorf("GetBinlogFromMysqlPosInfo getResult failed, err= %v ", err)
		return "", 0
	}
	if len(results) == 0 {
		log.Logger.Errorf("GetBinlogFromMysqlPosInfo getResult is nil, sql= %s, value=%v", sql, config.TargetMysqlAddr)
		return "", 0
	}
	return results[0]["slave_binlog_file_name"].(string), int(results[0]["slave_binlog_position"].(int64))
}

// GetSlaveInfo 获取从节点的info
func GetSlaveInfo() (string, string, string, string) {
	var slaveAddr string
	var slaveUser string
	var slavePasswd string
	masterAddr := healthcheck.GetMasterAddr()
	cfg := config.ConfigInfo
	if cfg.CanalConfig.Addr == masterAddr {
		slaveAddr = cfg.StandbyConfig.Addr
		slaveUser = cfg.StandbyConfig.User
		slavePasswd = cfg.StandbyConfig.Password
	} else {
		slaveAddr = cfg.CanalConfig.Addr
		slaveUser = cfg.CanalConfig.User
		slavePasswd = cfg.CanalConfig.Password
	}
	return masterAddr, slaveAddr, slaveUser, slavePasswd
}

// GetPosFromSlave 在从节点获取主从当前的pos，写入到数据库表mysql_pos_info
func GetPosFromSlave() (MysqlPosInfo, error) {
	masterAddr, slaveAddr, slaveUser, slavePasswd := GetSlaveInfo()

	var masterStatus []MasterStatus
	connCfg := fmt.Sprintf("%s:%s@tcp(%s)/mysql", slaveUser, slavePasswd, slaveAddr)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("GetPosFromSlave connect mysql failed,  err= %v ", err)
		return MysqlPosInfo{}, fmt.Errorf("GetPosFromSlave connect mysql failed")
	}
	defer conn.Close()

	sql := fmt.Sprintf("show master status")
	err = conn.Select(&masterStatus, sql)
	if err != nil {
		log.Logger.Errorf("get master status failed, sql = %s, err = %v", sql, err)
		return MysqlPosInfo{}, fmt.Errorf("GetPosFromSlave exec show master status failed")
	}
	slaveFile := masterStatus[0].File
	slavePos := masterStatus[0].Position
	log.Logger.Debugf("GetPosFromSlave slave master status file: %s, pos: %d ", slaveFile, slavePos)
	sql = fmt.Sprintf("show slave status")
	query, err := conn.Query(sql)
	if err != nil {
		log.Logger.Errorf("GetPosFromSlave exec sql= %s failed, err= %v ", sql, err)
		return MysqlPosInfo{}, fmt.Errorf("GetPosFromSlave exec show slave status failed")
	}

	results, err := getResult(query)
	if err != nil {
		log.Logger.Errorf("GetPosFromSlave getResult failed, err= %v ", err)
		return MysqlPosInfo{}, fmt.Errorf("GetPosFromSlave get show slave status result failed")
	}
	if len(results) == 0 {
		log.Logger.Errorf("GetPosFromSlave getResult from slave nil")
		return MysqlPosInfo{}, fmt.Errorf("GetPosFromSlave getResult from slave nil")
	}
	masterFile := results[0]["Relay_Master_Log_File"]
	masterPos := results[0]["Exec_Master_Log_Pos"]
	log.Logger.Debugf("GetPosFromSlave Relay_Master_Log_File: %s Exec_Master_Log_Pos: %d ", masterFile, masterPos)

	posInfo := MysqlPosInfo{
		SID:              config.ConfigInfo.RuleConfig.SID,
		MasterMysqlAddr:  masterAddr,
		MasterBinlogFile: masterFile,
		MasterBinlogPos:  masterPos,
		SlaveMysqlAddr:   slaveAddr,
		SlaveBinlogFile:  slaveFile,
		SlaveBinlogPos:   slavePos,
	}
	return posInfo, nil
}

func getResult(query *sql.Rows) ([]map[string]interface{}, error) {
	column, _ := query.Columns()          //读出查询出的列字段名
	columnTypes, _ := query.ColumnTypes() //读出查询出的列字段名

	values := make([]interface{}, len(column)) //values是每个列的值，这里获取到byte里
	scans := make([]interface{}, len(column))  //因为每次查询出来的列是不定长的，用len(column)定住当次查询的长度
	for i := range values {                    //让每一行数据都填充到[][]byte里面
		scans[i] = &values[i]
	}
	var results []map[string]interface{} //最后得到的map
	for query.Next() {                   //循环，让游标往下移动
		if err := query.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			fmt.Println(err)
			return nil, err
		}
		row := make(map[string]interface{}) //每行数据
		for k, v := range values {          //每行数据是放在values里面，现在把它挪到row里
			key := column[k]
			row[key] = dealData(v, columnTypes[k].DatabaseTypeName())
		}
		results = append(results, row) //装入结果集中
	}

	return results, nil

}
func dealData(v interface{}, t string) interface{} {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		cr := fmt.Sprintf("%s", v.([]byte))
		switch t {
		case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "YEAR":
			value, _ := strconv.ParseInt(cr, 10, 64)
			return value
		case "FLOAT", "DOUBLE":
			value, _ := strconv.ParseFloat(cr, 64)

			return value
		default:
			return cr
		}
	}
	return v
}

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

// DelPosTable 保留最近的ReserveHours小时的数据
func DelPosTable() {
	conn, err := getConnection()
	if err != nil {
		log.Logger.Fatalf("get connection from rule mysql err! exit!")
		return
	}
	defer conn.Close()
	reserveHours := config.ConfigInfo.AppConfig.ReserveHours
	//如果没有配置，默认为24小时
	if reserveHours == 0 {
		reserveHours = 24
	}
	timeBefore := reserveHours * 3600
	sql := fmt.Sprintf("delete from mysql_pos_info where sid=%d and insert_timestamp<unix_timestamp(now())-%d limit 100", config.ConfigInfo.RuleConfig.SID, timeBefore)
	for {
		_, err := conn.Exec(sql)
		if err != nil {
			log.Logger.Errorf("DelPosTable exec sql failed, sql:%s, err:%v", sql, err)
		}
		time.Sleep(10 * time.Second)
	}
}

// MonitorMysqlPos 每秒获取一次mysql主从pos对应关系，写入到数据库表mysql_pos_info,便于主从切换时找到从节点对应的pos位置
func MonitorMysqlPos() {
	log.Logger.Infof("MonitorMysqlPos thread started......")
	cfg := config.ConfigInfo
	if len(cfg.StandbyConfig.Addr) == 0 || len(cfg.StandbyConfig.User) == 0 || len(cfg.StandbyConfig.Password) == 0 {
		log.Logger.Errorf("StandbyConfig is not config in config file! MonitorMysqlPos thread exit!")
		return
	}
	conn, err := getConnection()
	if err != nil {
		log.Logger.Fatalf("get connection from rule mysql err! MonitorMysqlPos thread exit!")
		return
	}

	defer conn.Close()
	stmt, err := conn.Prepare("insert into mysql_pos_info (sid, master_mysql_addr, master_binlog_file_name, master_binlog_position, slave_mysql_addr, slave_binlog_file_name, slave_binlog_position, insert_timestamp) value(?,?,?,?,?,?,?,unix_timestamp(now()))")
	if err != nil {
		log.Logger.Fatalf("prepare sql failed")
		return
	}
	//起协程删除数据
	go DelPosTable()

	posSaveInterval := config.ConfigInfo.AppConfig.MysqlPosSaveInterval
	//如果没有配置，默认为1000ms
	if posSaveInterval == 0 {
		posSaveInterval = 1000
	}
	for {
		posInfo, err := GetPosFromSlave()
		if err != nil {
			log.Logger.Errorf("GetPosFromSlave failed: %v", err)
		} else {
			_, err = stmt.Exec(posInfo.SID, posInfo.MasterMysqlAddr, posInfo.MasterBinlogFile, posInfo.MasterBinlogPos,
				posInfo.SlaveMysqlAddr, posInfo.SlaveBinlogFile, posInfo.SlaveBinlogPos)
			if err != nil {
				log.Logger.Errorf("Exec insert into mysql_pos_info failed: %v", err)
			}
		}
		time.Sleep(time.Duration(posSaveInterval) * time.Millisecond)

	}
}
