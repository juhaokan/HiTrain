package ruleparser

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	log "hisense.com/fusion/logger"

	"github.com/jmoiron/sqlx"
	//"github.com/siddontang/go-log/log"
)

var (
	// TOAddRule 添加规则
	TOAddRule = 3
	// TODelRule 删除规则
	TODelRule = 4
	// RuleInitial 规则初始化，可以直接加载到内存中
	RuleInitial = 0
)

var mutex sync.Mutex
var dumpEnable = 1

// FusionRules 解析规则
var FusionRules = map[string][]Rule{}

// KafkaKeyMap key: db.tb   v: mysql中kafka_key_columns， 针对多partition场景，将column的值写到topic的key
var KafkaKeyMap = map[string]string{}

// KafkaTopicMap key: db.tb v: column_name   针对多topic场景, 表示此表需要多topic
var KafkaTopicMap = map[string]string{}

// KafkaTopicRuleMap key: db.tb.column_value v: topic   针对多topic场景，不同的column路由到不同的topic
var KafkaTopicRuleMap = map[string]string{}

func checkFilterRule(filterRule string) error {
	if strings.Replace(filterRule, " ", "", -1) == "" {
		return nil
	}
	ruleList := strings.Split(filterRule, ",")
	for _, filter := range ruleList {
		e := strings.Split(filter, "=")
		if len(e) != 2 {
			log.Logger.Errorf("filter_rule config err, filter_rule=%s", filterRule)
			return fmt.Errorf("filter_rule config err, filter_rule=%s", filterRule)
		}
	}
	return nil
}

func checkRules(rules []Rule) error {
	errMsg := ""
	for _, rule := range rules {
		if rule.RefreshType != 0 && rule.RefreshType != 1 {
			errMsg = errMsg + fmt.Sprintf("id = %d, RefreshType should be 0 or 1, RefreshType  = %d\n", rule.ID, rule.RefreshType)
		}
		if rule.ISDump != 0 && rule.ISDump != 1 {
			errMsg = errMsg + fmt.Sprintf("id = %d, ISDump should be 0 or 1, ISDump  = %d\n", rule.ID, rule.ISDump)
		}
		if rule.ISCompress < 0 || rule.ISCompress > 4 {
			errMsg = errMsg + fmt.Sprintf("id = %d, ISCompress range should in[0,4], ISCompress  = %d\n", rule.ID, rule.ISCompress)
		}
		if rule.ExpireTime <= 0 {
			errMsg = errMsg + fmt.Sprintf("id = %d, ExpireTime is not bigger than 0, ExpireTime = %d\n", rule.ID, rule.ExpireTime)
		}
		if rule.ExpireRange > 50 || rule.ExpireRange < 0 {
			errMsg = errMsg + fmt.Sprintf("id = %d, ExpireRange range should in[0,50] , ExpireRange = %d\n", rule.ID, rule.ExpireRange)
		}
		if rule.ISFullColumn != 0 && rule.ISFullColumn != 1 {
			errMsg = errMsg + fmt.Sprintf("id = %d, ISFullColumn is not 0 or 1, ISFullColumn = %d\n", rule.ID, rule.ISFullColumn)
		}
		keyPrefix := rule.RedisKeyPrefix
		keyPrefix = strings.Replace(keyPrefix, " ", "", -1)
		if keyPrefix == "" {
			errMsg = errMsg + fmt.Sprintf("id = %d, RedisKeyPrefix is empty string\n", rule.ID)
		}
		if rule.RedisKeyType != "string" && rule.RedisKeyType != "set" && rule.RedisKeyType != "hash" && rule.RedisKeyType != "zset" {
			errMsg = errMsg + fmt.Sprintf("id = %d, RedisKeyType is err, RedisKeyType = %s\n", rule.ID, rule.RedisKeyType)
		}
		if err := checkFilterRule(rule.FilterRule); err != nil {
			errMsg = errMsg + fmt.Sprintf("id = %d, filter_rule is err\n", rule.ID)
		}
	}

	if errMsg != "" {
		log.Logger.Errorf("checkRules got error, err=%s", errMsg)
		return fmt.Errorf("check rules error")
	}
	return nil
}

func getRuleConfig(cfg *RuleConfig) []Rule {
	columns := "id,sid,db,tb,refresh_type,is_dump,is_compress,compress_level,compress_columns,expire_time,expire_range,is_full_column,replic_columns,redis_key_prefix,redis_key_columns,redis_key_type,redis_zset_score_column,filter_rule,operation,kafka_key_columns,kafka_topic_rule,delay_del,delay_expire_time,kafka_is_full_column,kafka_replic_columns"
	sql := fmt.Sprintf("select %s from %s where sid = %d and operation = %d", columns, cfg.Table, cfg.SID, RuleInitial)
	return executeSQL(cfg, sql)
}

func executeSQL(cfg *RuleConfig, sql string) []Rule {
	rules := []Rule{}
	connCfg := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Addr, cfg.Port, cfg.Schema)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		fmt.Printf("err = %v", err)
		log.Logger.Errorf("new connection to %s failed, err= %v ", connCfg, err)
		return nil
	}
	defer conn.Close()
	err = conn.Select(&rules, sql)
	if err != nil {
		fmt.Printf("select err = %v", err)
		log.Logger.Errorf("get Rule Failed, sql = %s", sql)
		return nil
	}
	return rules
}

// GetRules 获取规则
func GetRules(key string) []Rule {
	mutex.Lock()
	defer mutex.Unlock()
	rules, ok := FusionRules[key]
	if !ok {
		return nil
	}
	return rules
}

// GetCanalFilterTables 组装需要同步表的信息
func GetCanalFilterTables() (map[string][]string, []string, []string) {
	dumpTables := make(map[string][]string)
	includeTables := []string{}
	excludeTables := []string{}
	mutex.Lock()
	defer mutex.Unlock()
	for key, rules := range FusionRules {
		for _, rule := range rules {
			if rule.ISDump == dumpEnable {
				_, ok := dumpTables[rule.DB]
				if !ok {
					dumpTables[rule.DB] = []string{rule.TB}
				} else {
					dumpTables[rule.DB] = append(dumpTables[rule.DB], rule.TB)
				}
				break
			}
		}
		includeTables = append(includeTables, key)
	}
	return dumpTables, includeTables, excludeTables
}

// ResetOperationColumn 将operation 字段置成 initial state
func ResetOperationColumn(cfg *RuleConfig) error {
	connCfg := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Addr, cfg.Port, cfg.Schema)
	conn, err := sqlx.Open("mysql", connCfg)
	if err != nil {
		log.Logger.Errorf("new connection to %s failed, err= %v ", connCfg, err)
		return err
	}
	defer conn.Close()
	sqlResetOperation := fmt.Sprintf("update %s set operation = %d where sid = %d and operation = %d ", cfg.Table, RuleInitial, cfg.SID, TOAddRule)
	sqlDeleteOperation := fmt.Sprintf("delete from %s where sid = %d and operation = %d ", cfg.Table, cfg.SID, TODelRule)

	_, err = conn.Exec(sqlResetOperation)
	if err != nil {
		log.Logger.Errorf("reset column operaion to 0 failed, err= %v", err)
		return err
	}
	_, err = conn.Exec(sqlDeleteOperation)
	if err != nil {
		log.Logger.Errorf("delete column operaion to 0 failed, err= %v", err)
		return err
	}
	return nil
}

// MonitorRuleChange 监听规则变化
func MonitorRuleChange(cfg *RuleConfig, toKafka int) (bool, string, string) {
	var sql string
	var dumpTables []string
	var delTables []string
	restartCanal := false

	columns := "id,sid,db,tb,refresh_type,is_dump,is_compress,compress_level,compress_columns,expire_time,expire_range,is_full_column,replic_columns,redis_key_prefix,redis_key_columns,redis_key_type,redis_zset_score_column,filter_rule,operation,kafka_key_columns,kafka_topic_rule,delay_del,delay_expire_time,kafka_is_full_column,kafka_replic_columns"
	sql = fmt.Sprintf("select %s from %s where sid = %d and (operation=%d or operation=%d)", columns, cfg.Table, cfg.SID, TOAddRule, TODelRule)

	rules := executeSQL(cfg, sql)
	if rules == nil || len(rules) == 0 {
		return false, "", ""
	}
	if NeedKafka == 0 || NeedKafka == 2 {
		if err := checkRules(rules); err != nil {
			return restartCanal, "", ""
		}
	}

	mutex.Lock()
	for _, rule := range rules {
		restartCanal = true
		key := fmt.Sprintf("%s.%s", rule.DB, rule.TB)
		if _, ok := FusionRules[key]; !ok {
			if rule.OP == TOAddRule {
				FusionRules[key] = []Rule{rule}
				if rule.ISDump == dumpEnable {
					dumpTables = append(dumpTables, key)
				}
				err := addGlobalMap(key, rule)
				if err != nil {
					return false, "", ""
				}
			} else {
				log.Logger.Errorf("op config failed, op = %d, if fusion just start, ignore it", rule.OP)
			}
		} else {
			if rule.OP == TOAddRule {
				FusionRules[key] = append(FusionRules[key], rule)
				if rule.ISDump == dumpEnable {
					dumpTables = append(dumpTables, key)
				}
				err := addGlobalMap(key, rule)
				if err != nil {
					return false, "", ""
				}
			}
			if rule.OP == TODelRule {
				for idx, oRule := range FusionRules[key] {
					if oRule.ID == rule.ID {
						if len(FusionRules[key]) == 1 {
							delete(FusionRules, key)
							deleteGlobalMap(key, rule)
						} else {
							FusionRules[key] = append(FusionRules[key][0:idx], FusionRules[key][idx+1:]...)
						}
						delTables = append(delTables, key)
						break
					}
				}
			}
		}
	}
	log.Logger.Debugf("new rule config = %v", FusionRules)
	mutex.Unlock()
	return restartCanal, strings.Join(dumpTables, ","), strings.Join(delTables, ",")
}

func addGlobalMap(key string, rule Rule) error {
	if rule.KafkaKeyColumns != "" {
		KafkaKeyMap[key] = rule.KafkaKeyColumns
	}
	if rule.KafkaTopicRule != "" {
		var r KafkaTopicRule
		err := json.Unmarshal([]byte(rule.KafkaTopicRule), &r)
		if err != nil {
			log.Logger.Errorf("KafkaTopicRule Json Unmarshal err:%s\n", err)
			return err
		}
		KafkaTopicMap[key] = r.ColumnName
		data, _ := r.TopicRule.(map[string]interface{})
		for columnValue, topic := range data {
			log.Logger.Debugf("columnValue:%s, topic:%s\n", columnValue, topic)
			k := fmt.Sprintf("%s.%s.%s", rule.DB, rule.TB, columnValue)
			KafkaTopicRuleMap[k] = topic.(string)
		}

	}
	return nil
}

func deleteGlobalMap(key string, rule Rule) {
	if rule.KafkaKeyColumns != "" {
		delete(KafkaKeyMap, key)
	}
	if rule.KafkaTopicRule != "" {
		var r KafkaTopicRule
		err := json.Unmarshal([]byte(rule.KafkaTopicRule), &r)
		if err != nil {
			log.Logger.Errorf("KafkaTopicRule Json Unmarshal err:%s\n", err)

		}
		delete(KafkaTopicMap, key)
		data, _ := r.TopicRule.(map[string]interface{})
		for columnValue, topic := range data {
			log.Logger.Debugf("columnValue:%s, topic:%s\n", columnValue, topic)
			k := fmt.Sprintf("%s.%s.%s", rule.DB, rule.TB, columnValue)
			delete(KafkaTopicRuleMap, k)
		}

	}

}

// NewRuleParser 重数据库中获取redis 存储规则
func NewRuleParser(cfg *RuleConfig) error {
	rules := getRuleConfig(cfg)
	if rules == nil {
		return fmt.Errorf("get rule failed")
	}
	if NeedKafka == 0 || NeedKafka == 2 {
		if len(rules) == 0 {
			return fmt.Errorf("get rule failed, rule is nil")
		}
		if err := checkRules(rules); err != nil {
			return err
		}
	}

	for _, rule := range rules {
		key := fmt.Sprintf("%s.%s", rule.DB, rule.TB)
		if _, ok := FusionRules[key]; !ok {
			FusionRules[key] = []Rule{rule}
		} else {
			FusionRules[key] = append(FusionRules[key], rule)
		}
		err := addGlobalMap(key, rule)
		if err != nil {
			return err
		}
	}
	log.Logger.Infof("get FusionRules = %+v", FusionRules)
	return nil
}
