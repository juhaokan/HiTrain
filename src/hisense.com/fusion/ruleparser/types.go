package ruleparser

// RuleConfig 在数据表中存储rule 规则
type RuleConfig struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Port     int    `toml:"port"`
	SID      int    `toml:"sid"`
	Schema   string `toml:"schema"`
	Table    string `toml:"table"`
}

// Rule 配置的同步规则
type Rule struct {
	ID                 int    `db:"id"`
	SID                int    `db:"sid"`
	DB                 string `db:"db"`
	TB                 string `db:"tb"`
	RefreshType        int    `db:"refresh_type"`
	ISDump             int    `db:"is_dump"`
	ISCompress         int    `db:"is_compress"`
	CompressLevel      int    `db:"compress_level"`
	CompressColumns    string `db:"compress_columns"`
	ExpireTime         int    `db:"expire_time"`
	ExpireRange        int    `db:"expire_range"`
	ISFullColumn       int    `db:"is_full_column"`
	ReplicColumns      string `db:"replic_columns"`
	FilterRule         string `db:"filter_rule"`
	RedisKeyPrefix     string `db:"redis_key_prefix"`
	RedisKeyColumns    string `db:"redis_key_columns"`
	RedisKeyType       string `db:"redis_key_type"`
	ZSetColumn         string `db:"redis_zset_score_column"`
	OP                 int    `db:"operation"`
	KafkaKeyColumns    string `db:"kafka_key_columns"`
	KafkaTopicRule     string `db:"kafka_topic_rule"`
	DelayDel           int    `db:"delay_del"`
	DelayExpireTime    int    `db:"delay_expire_time"`
	KafkaISFullColumn  int    `db:"kafka_is_full_column"`
	KafkaReplicColumns string `db:"kafka_replic_columns"`
}

// KafkaTopicRule 规则表中KafkaTopicRule
type KafkaTopicRule struct {
	ColumnName string      `json:"column_name"`
	TopicRule  interface{} `json:"topic_rule"`
}

//NeedKafka 表示是否将解析数据发送到kafka， 如果需要发送的kafka, 不对rule 规则进行校验,
var NeedKafka = 0
