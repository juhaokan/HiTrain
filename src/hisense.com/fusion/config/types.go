package config

import (
	"time"

	"hisense.com/fusion/redisutil"
	"hisense.com/fusion/ruleparser"

	"github.com/siddontang/go-mysql/canal"
)

// AppConfig app 配置，端口号用于提供http服务
type AppConfig struct {
	Port                  int    `toml:"port"`
	SaveInterval          int64  `toml:"save_position_interval"`
	FailoverMAXCheck      int    `toml:"failover_max_check"`
	Heartbeat             string `toml:"heartbeat"`
	MysqlVersion          string `toml:"mysql_version"`
	ISSyncFromSlave       int    `toml:"is_sync_from_slave"`
	ISSupportDDL          int    `toml:"is_support_ddl"`
	ISSupportDefineColumn int    `toml:"is_support_define_column"`
	MysqlSwitchType       int    `toml:"mysql_switch_type"`
	ReserveHours          int    `toml:"reserve_hours"`
	MysqlPosSaveInterval  int64  `toml:"mysql_pos_save_interval"`
	FusionStartInterval   int    `toml:"fusion_start_interval"`
}

// FusionConfig config file
type FusionConfig struct {
	CanalConfig    *canal.Config          `toml:"canal"`
	RDSSlaveConfig *RDSSlaveConfig        `toml:"rds_slave"`
	DumpConfig     *canal.DumpConfig      `toml:"dump"`
	RedisConfig    *redisutil.RedisConfig `toml:"redis"`
	RuleConfig     *ruleparser.RuleConfig `toml:"rule"`
	AppConfig      *AppConfig             `toml:"app"`
	KafkaConfig    *KafkaConfig           `toml:"kafka"`
	StandbyConfig  *StandbyConfig         `toml:"standby"`
	LogConfig      *LogConfig             `toml:"log"`
}

// KafkaConfig kafka config
type KafkaConfig struct {
	NeedKafka        int           `toml:"need_kafka"`
	DataFormat       int           `toml:"data_format"`
	BrokerList       string        `toml:"broker_list"`
	Topic            string        `toml:"topic"`
	FlushMAXMessages int           `toml:"flush_max_messages"`
	FlushFrequency   time.Duration `toml:"flush_frequency"`
	MaxRequestSize   int           `toml:"max_request_size"`
	MaxMessageBytes  int           `toml:"max_message_bytes"`
}

// StandbyConfig mysql 备库配置
type StandbyConfig struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

// RDSSlaveConfig RDS只读副本配置
type RDSSlaveConfig struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

// LogConfig
type LogConfig struct {
	LogLevel string `toml:"level"`
}
