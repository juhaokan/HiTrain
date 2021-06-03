package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"hisense.com/fusion/app"
	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer"
	"hisense.com/fusion/dataparser"
	"hisense.com/fusion/dump"
	"hisense.com/fusion/healthcheck"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/rds"
	"hisense.com/fusion/redisutil"
	"hisense.com/fusion/ruleparser"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	//workingFlag   = 0
	fusionRunning     = 1
	fusion            *canal.Canal
	contextCanalError = "context canceled"
	errPos            = "errpos"
)

func newCanalCFG(cfg *config.FusionConfig, addr, user, password string) canal.Config {
	return canal.Config{
		Addr:                  addr,
		User:                  user,
		Password:              password,
		Charset:               cfg.CanalConfig.Charset,
		ServerID:              cfg.CanalConfig.ServerID,
		Flavor:                cfg.CanalConfig.Flavor,
		HeartbeatPeriod:       cfg.CanalConfig.HeartbeatPeriod,
		ReadTimeout:           cfg.CanalConfig.ReadTimeout,
		IncludeTableRegex:     cfg.CanalConfig.IncludeTableRegex,
		ExcludeTableRegex:     cfg.CanalConfig.ExcludeTableRegex,
		DiscardNoMetaRowEvent: cfg.CanalConfig.DiscardNoMetaRowEvent,
		Dump:            cfg.CanalConfig.Dump,
		UseDecimal:      cfg.CanalConfig.UseDecimal,
		SemiSyncEnabled: cfg.CanalConfig.SemiSyncEnabled,
	}
}

func startRDS(cfg *config.FusionConfig) *canal.Canal {
	// 判断是否是rds，全量的话需要在只读副本上全量dump
	rdsfile, rdspos, err := rds.RDSSync(cfg, false)
	if err != nil {
		fmt.Printf("RDSSync failed, err = %v ", err)
		log.Logger.Fatalf("RDSSync failed, err = %v ", err)
		return nil
	}

	user, password, addr := healthcheck.GetTargetMysql()
	canalCfg := newCanalCFG(cfg, addr, user, password)
	c, err := canal.NewCanal(&canalCfg)
	if err != nil {
		fmt.Printf("new canal failed ! system exit, err = %v ", err)
		log.Logger.Fatalf("new canal failed ! system exit, err = %v ", err)
		return nil
	}
	fusion = c
	// Register a handler to handle RowsEvent
	c.SetEventHandler(&dataparser.MyEventHandler{})

	if len(rdsfile) != 0 && rdspos != 0 {
		log.Logger.Infof("Canal begin RunFrom %s:%d.", rdsfile, rdspos)
		err := c.RunFrom(mysql.Position{Name: rdsfile, Pos: rdspos})
		if err != nil && err.Error() != contextCanalError {
			fmt.Printf("canal run  failed ! err = %v ", err)
			log.Logger.Fatalf("canal run  failed ! err = %v ", err)
		}

		return nil
	}
	return c
}

// 从库必须配置成只读，否则对fusion的连接会有影响（获取要连接数据库错误）
func startFusion(cfg *config.FusionConfig, failover bool) {
	//workingFlag = 1
	log.Logger.Infof("startFusion beging. failover:%v, sid=%d, get config file data = %s", failover, cfg.RuleConfig.SID, config.PrintConf(cfg))

	//判断是否是RDS，RDS特殊处理
	c := startRDS(cfg)
	if c == nil {
		return
	}

	//根据mysql持久化的位置来决定是否开启全量，如果binlogfile 为空， pos 位置为空，有表需要全量，则开始全量，否则走增量。
	mysqlAddr, binlog, pos := consumer.GetPos(config.SID)
	if binlog == errPos { // 该种情况基本不会发生
		log.Logger.Fatalf("get connection to sync from mysql failed")
	}

	//20210301解决线上mysql连接数打满，failover时若配置的is_dump全为0，错误的进行所有库表的全量同步问题
	if len(binlog) == 0 || pos == 0 {
		if len(cfg.CanalConfig.Dump.DBTBSMap) == 0 {
			binlog, pos := dump.GetMasterStatus()
			if len(binlog) == 0 {
				log.Logger.Fatalf("get master status failed, system exiting....")
			}
			log.Logger.Infof("binlog or pos is null, no dump table, RunFrom master status: %s:%d", binlog, pos)
			err := c.RunFrom(mysql.Position{Name: binlog, Pos: uint32(pos)})
			if err != nil && err.Error() != contextCanalError {
				fmt.Printf("canal RunFrom  failed ! err = %v ", err)
				log.Logger.Fatalf("canal RunFrom  failed ! err = %v ", err)
			}

		} else {
			log.Logger.Infof("binlog or pos is null, has dump table, begin Run.")
			err := c.Run()
			if err != nil && err.Error() != contextCanalError {
				fmt.Printf("canal run  failed ! err = %v ", err)
				log.Logger.Fatalf("canal run  failed ! err = %v ", err)
			}
		}

	} else {
		//发生主从切换场景
		if mysqlAddr != config.TargetMysqlAddr {
			log.Logger.Infof("working_info mysql addr is not master mysql addr now. working_info mysql:%s, binlog:%s, pos:%d, master mysql:%s", mysqlAddr, binlog, pos, config.TargetMysqlAddr)
			newBinlog, newPos := dump.GetNewMasterBinlogInfo(mysqlAddr, binlog, pos)
			log.Logger.Infof("mysqlAddr != config.TargetMysqlAddr mybe mysql switch. get new master binlog:%s, pos:%d", newBinlog, newPos)
			if newPos == 0 {
				fmt.Printf("GetNewMasterBinlogInfo get binlog info failed, system exiting....")
				log.Logger.Fatalf("GetNewMasterBinlogInfo get binlog info failed, system exiting....")
			}
			log.Logger.Infof("begin RunFrom %s:%d", newBinlog, newPos)
			err := c.RunFrom(mysql.Position{Name: newBinlog, Pos: uint32(newPos)})
			if err != nil && err.Error() != contextCanalError {
				fmt.Printf("canal run  failed ! err = %v ", err)
				log.Logger.Fatalf("canal run  failed ! err = %v ", err)
			}
		} else {
			log.Logger.Infof("binlog and pos is not null, begin RunFrom %s:%d", binlog, pos)
			err := c.RunFrom(mysql.Position{Name: binlog, Pos: uint32(pos)})
			if err != nil && err.Error() != contextCanalError {
				fmt.Printf("canal run  failed ! err = %v ", err)
				log.Logger.Fatalf("canal run  failed ! err = %v ", err)
			}
		}

	}
	//workingFlag = 0
}

func startHTTPServer(cfg *config.AppConfig) {
	port := cfg.Port
	router := app.NewRouter()
	addr := fmt.Sprintf(":%d", port)
	log.Logger.Infof("listen addr = %s ", addr)
	go http.ListenAndServe(addr, router)
}

func initConf() (*config.FusionConfig, error) {
	config.LogLevel = "info"
	if err := log.InitLogger(config.LogPath, config.LogLevel); err != nil {
		fmt.Printf("init logger failed, err = %v ", err)
		return nil, err
	}
	cfg := config.InitCfgUseFile()
	if cfg == nil {
		log.Logger.Errorf("get config file failed ")
		return nil, fmt.Errorf("get config file failed")
	}
	config.SID = cfg.RuleConfig.SID
	if err := log.SetLogLevel(cfg.LogConfig.LogLevel); err != nil {
		log.Logger.Errorf("get config log level is err, %v", err)
		return nil, err
	}
	return cfg, nil
}

func transFusionStartInterval(fusionStartInterval int) int {
	if fusionStartInterval == 0 {
		fusionStartInterval = 20
	}
	return fusionStartInterval
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	cfg, err := initConf()
	if err != nil {
		fmt.Printf("initConf err, %v", err)
		log.Logger.Errorf("initConf err, %v", err)
		return
	}
	ruleparser.NeedKafka = cfg.KafkaConfig.NeedKafka
	if err := ruleparser.NewRuleParser(cfg.RuleConfig); err != nil {
		fmt.Printf("get Rule Config failed, err = %v ", err)
		log.Logger.Errorf("get Rule Config failed, err = %v ", err)
		return
	}

	if cfg.KafkaConfig.NeedKafka == 0 || cfg.KafkaConfig.NeedKafka == 2 {
		if err := redisutil.NewRedisPool(cfg.RedisConfig); err != nil {
			fmt.Printf("init redis connection pool failed, err = %v ", err)
			log.Logger.Errorf("init redis connection pool failed, err = %v ", err)
			return
		}
	}

	dumpTables, includeTables, _ := dataparser.GetFilterTables()
	cfg.CanalConfig.Dump.DBTBSMap = dumpTables
	cfg.CanalConfig.IncludeTableRegex = includeTables

	// 将mysql 的变更 根据redis 规则， 写redis
	go consumer.ConsumeData(60)
	go startHTTPServer(cfg.AppConfig)
	dataparser.NewConsumerData()

	// 如果masterFile不为"",则从当前位置开启同步
	go startFusion(cfg, false)
	go healthcheck.MonitorTargetMysqlHealth()
	go consumer.SavePos(cfg.RuleConfig.SID)
	go dump.Monitor()
	// 每秒获取一次mysql主从pos对应关系，写入到数据库表mysql_pos_info
	go dump.MonitorMysqlPos()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc)
	for {
		select {
		case n := <-sc:
			if n == syscall.SIGKILL || n == syscall.SIGHUP || n == syscall.SIGINT || n == syscall.SIGTERM || n == syscall.SIGQUIT {
				fmt.Printf("receive signal %v, closing", n)
				log.Logger.Infof("receive signal %v, closing", n)
				return
			}
			if n == syscall.Signal(0xa) {
				log.IncLogLevel()
			}
			if n == syscall.Signal(0xc) {
				log.DecLogLevel()
			}
		case dumpFlag := <-dump.DumpFlagChan:
			switch dumpFlag {
			case dump.SetDumpFlag:
				log.Logger.Infof("SetDumpFlag is set, close fusion begin.")
				fusion.Close()
			case dump.ReleaseDumpFlag:
				log.Logger.Infof("ReleaseDumpFlag is set, startFusion begin.")
				go startFusion(cfg, false)
			case dump.AddRuleFlag, dump.DelRuleFlag, dump.RestartCanalFlag:
				cfg := config.InitCfgUseFile()
				if cfg == nil {
					log.Logger.Errorf("get config file failed ")
					return
				}
				dumpTables, includeTables, _ := dataparser.GetFilterTables()
				cfg.CanalConfig.Dump.DBTBSMap = dumpTables
				cfg.CanalConfig.IncludeTableRegex = includeTables
				log.Logger.Infof("dump RuleFlag is set, startFusion begin.")
				go startFusion(cfg, false)
				dump.RuleChan <- dump.RuleApplySuccess
			}
		case <-healthcheck.FailoverChan:
			log.Logger.Infof("receive mysql failover signal, ready to stop fusion ......")
			fusion.Close()
			fusionStartInterval := cfg.AppConfig.FusionStartInterval
			//如果没有配置，默认为20s
			transInterval := transFusionStartInterval(fusionStartInterval)
			log.Logger.Infof("stop fusion success, wait for %d seconds to restart fusion again !", transInterval)
			time.Sleep(time.Duration(transInterval) * time.Second)
			go startFusion(cfg, true)
		}
	}
}

func init() {
	flag.StringVar(&config.ConfigFile, "config", "../conf/fusiondataprocessing.conf", "fusiondataprocessing config file path")
	flag.StringVar(&config.LogPath, "logpath", "/var/log/fusion/fusion.log", "fusiondataprocessing log path")
}
