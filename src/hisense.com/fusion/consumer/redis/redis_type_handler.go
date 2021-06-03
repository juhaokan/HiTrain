package redis

import (
	"fmt"
	"strings"

	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/redisutil"
	"hisense.com/fusion/ruleparser"

	"github.com/garyburd/redigo/redis"
	//"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
)

func handleStringType(rule ruleparser.Rule, conn redis.Conn, action string, redisKey string, value interface{}, expireTime int) error {
	var err error
	if action == canal.InsertAction || action == canal.UpdateAction {
		_, err = conn.Do("SETEX", redisKey, expireTime, value)
	}
	if action == canal.DeleteAction {
		if rule.DelayDel == 0 {
			_, err = conn.Do("DEL", redisKey)
		} else {
			_, err = conn.Do("EXPIRE", redisKey, rule.DelayExpireTime)
		}
	}
	if err != nil {
		log.Logger.Errorf("action = %s failed key = %s, value = %v failed, err = %v ", action, redisKey, value, err)
		return err
	}
	log.Logger.Infof("action = %s success, key = %s success ", action, redisKey)
	log.Logger.Debugf("action = %s success, key = %s, v = %v  success ", action, redisKey, value)
	return nil
}

func handleSetType(conn redis.Conn, action string, redisKey string, value interface{}, expireTime int) error {
	var err error
	if action == canal.InsertAction || action == canal.UpdateAction {
		_, err = conn.Do("SADD", redisKey, value)
	}
	if action == canal.DeleteAction {
		_, err = conn.Do("SREM", redisKey, value)
	}
	if err != nil {
		log.Logger.Errorf("redis cmd exe err: action = %s, key = %s, v=%v, err = %v ", action, redisKey, value, err)
		return err
	}
	_, err = conn.Do("EXPIRE", redisKey, expireTime)
	if err != nil {
		log.Logger.Errorf("redis set EXPIRE err: action = %s, key = %s, value = %v, err = %v ", action, redisKey, value, err)
		return err
	}
	log.Logger.Infof("action = %s success, key = %s success ", action, redisKey)
	log.Logger.Debugf("action = %s success, key = %s, v = %v  success ", action, redisKey, value)
	return nil
}

func handleZSetType(conn redis.Conn, action string, redisKey string, value interface{}, score interface{}, expireTime int) error {
	var err error
	if action == canal.InsertAction || action == canal.UpdateAction {
		_, err = conn.Do("ZADD", redisKey, fmt.Sprintf("%v", score), value)
	}
	if action == canal.DeleteAction {
		_, err = conn.Do("ZREM", redisKey, value)
	}
	if err != nil {
		log.Logger.Errorf("redis cmd exe err: action = %s, key = %s, v=%v, err = %v ", action, redisKey, value, err)
		return err
	}
	_, err = conn.Do("EXPIRE", redisKey, expireTime)
	if err != nil {
		log.Logger.Errorf("redis set EXPIRE err: action = %s, key = %s, value = %v, err = %v ", action, redisKey, value, err)
		return err
	}
	log.Logger.Infof("action = %s success, key = %s success ", action, redisKey)
	log.Logger.Debugf("action = %s success, key = %s, v = %v  success ", action, redisKey, value)

	return nil
}

func handleHashType(rule ruleparser.Rule, conn redis.Conn, action string, redisKey string, value map[string]interface{}, expireTime int) error {
	var err error
	if action == canal.InsertAction || action == canal.UpdateAction {
		_, err = conn.Do("HMSET", redis.Args{}.Add(redisKey).AddFlat(value)...)
		if err != nil {
			log.Logger.Errorf("HMSET action failed, key = %s, v=%v ", redisKey, value)
			return err
		}
		_, err = conn.Do("EXPIRE", redisKey, expireTime)
	}
	if action == canal.DeleteAction {
		if rule.DelayDel == 0 {
			_, err = conn.Do("DEL", redisKey)
		} else {
			_, err = conn.Do("EXPIRE", redisKey, rule.DelayExpireTime)
		}
	}
	if err != nil {
		log.Logger.Errorf("action = %s failed key = %s, value = %v failed, err = %v ", action, redisKey, value, err)
		return err
	}
	log.Logger.Infof("action = %s success, key = %s success ", action, redisKey)
	log.Logger.Debugf("action = %s success, key = %s, v = %v  success ", action, redisKey, value)
	return nil
}

// isFilterMatch 只有配置了，没有匹配才成功能返回false, 否则都返回true
func isFilterMatch(rule ruleparser.Rule, data []utility.ColumnData) (bool, map[int]string) {
	indexMap := make(map[int]string)
	filters := rule.FilterRule
	if len(filters) == 0 {
		log.Logger.Debugf("return value = %v, index_map = %+v", true, indexMap)
		return true, indexMap
	}
	f := trimKeysString(filters, ",")
	for _, v := range f {
		col := strings.Split(v, "=")
		if len(col) != 2 {
			log.Logger.Errorf("filter rule config error, rule = %s", filters)
			log.Logger.Debugf("return value = %v, index_map = %+v", true, indexMap)
			return true, indexMap
		}
		colk := strings.TrimSpace(col[0])
		colv := strings.TrimSpace(col[1])
		for idx, d := range data {
			_v := addValue(d.Value)
			if d.Name == colk {
				indexMap[idx] = _v
				if _v != colv {
					log.Logger.Debugf("return value = %v, index_map = %+v", false, indexMap)
					return false, indexMap
				}
			}
		}
	}
	log.Logger.Debugf("return value = %v, index_map = %+v", true, indexMap)
	return true, indexMap
}

func handleRedisType(rule ruleparser.Rule, data utility.ConsumerDataStruct) error {
	keyColumns := trimKeysString(rule.RedisKeyColumns, ",")
	action := data.Action
	conn := redisutil.Pool.Get()
	defer conn.Close()
	for _, data := range data.ColumnData {
		ok, _ := isFilterMatch(rule, data)
		if !ok {
			continue
		}
		var redisKey string
		key := getColumnKey(keyColumns, data)
		if len(key) > 0 {
			if rule.RedisKeyType == RedisKeyTypeSet || rule.RedisKeyType == RedisKeyTypeZset {
				log.Logger.Warnf("set or zset redis type, RedisKeyColumns should be null")
				redisKey = rule.RedisKeyPrefix
			} else {
				redisKey = fmt.Sprintf("%s:%s", rule.RedisKeyPrefix, key)
			}
		} else {
			redisKey = rule.RedisKeyPrefix
		}
		if rule.RefreshType == RefreshTypeDel {
			_, err := conn.Do("DEL", redisKey)
			if err != nil {
				log.Logger.Errorf("delete key = %s failed, err = %v ", key, err)
				return err
			}
			log.Logger.Infof("to del redis key = %s success ", redisKey)
			continue
		}
		expireTime := getExpiredTime(rule)

		switch rule.RedisKeyType {
		case RedisKeyTypeString:
			v := getColumnStringValue(rule, data)
			if err := handleStringType(rule, conn, action, redisKey, v, expireTime); err != nil {
				return err
			}
		case RedisKeyTypeHash:
			v := getColumnHashValue(rule, data)
			if err := handleHashType(rule, conn, action, redisKey, v, expireTime); err != nil {
				return err
			}
		case RedisKeyTypeSet:
			v := getColumnStringValue(rule, data)
			if err := handleSetType(conn, action, redisKey, v, expireTime); err != nil {
				return err
			}
		case RedisKeyTypeZset:
			v := getColumnStringValue(rule, data)
			score := getColumnScoreValue(rule, data)
			if err := handleZSetType(conn, action, redisKey, v, score, expireTime); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupport redistype=[%s]", rule.RedisKeyType)
		}
	}
	return nil
}

func getRedisKey(rule ruleparser.Rule, key string) string {
	var redisKey string
	if len(key) > 0 {
		if rule.RedisKeyType == RedisKeyTypeSet || rule.RedisKeyType == RedisKeyTypeZset {
			log.Logger.Warnf("set or zset redis type, RedisKeyColumns should be null")
			redisKey = rule.RedisKeyPrefix
		} else {
			redisKey = fmt.Sprintf("%s:%s", rule.RedisKeyPrefix, key)
		}
	} else {
		redisKey = rule.RedisKeyPrefix
	}
	return redisKey
}

// zset, set 结构需要清理，其他只用使用update 函数即可实现redis key 对应的最新值, 配置根据字段过滤规则的，如果配置的过滤字段改变了，也需要清理。
// 20191202新增： 针对yangguang4新需求，如果keyColumns中有一个变化了，也需要清理，保持和数据库完全一致。
func clearRedisOldData(rule ruleparser.Rule, data utility.ConsumerDataStruct) error {
	keyColumns := trimKeysString(rule.RedisKeyColumns, ",")
	conn := redisutil.Pool.Get()
	defer conn.Close()
	for idx, _data := range data.ColumnDataBefore {
		ok, indexMap := isFilterMatch(rule, _data)
		if !ok {
			continue
		}
		var redisKey string
		var err error
		key := getColumnKey(keyColumns, _data)
		newKey := getColumnKey(keyColumns, data.ColumnData[idx])
		redisKey = getRedisKey(rule, key)

		switch rule.RedisKeyType {
		case RedisKeyTypeSet:
			v := getColumnStringValue(rule, _data)
			_, err = conn.Do("SREM", redisKey, v)
			if err != nil {
				log.Logger.Errorf("clear redis old data failed key = %s, value = %v failed, err = %v ", redisKey, v, err)
				return err
			}
		case RedisKeyTypeZset:
			v := getColumnStringValue(rule, _data)
			_, err = conn.Do("ZREM", redisKey, v)
			if err != nil {
				log.Logger.Errorf("clear redis old data failed key = %s, value = %v failed, err = %v ", redisKey, v, err)
				return err
			}
		case RedisKeyTypeString, RedisKeyTypeHash:
			delFlag := false
			for key, value := range indexMap {
				if addValue(data.ColumnData[idx][key].Value) != value {
					delFlag = true
					break
				}
			}
			if delFlag || (key != newKey) {
				_, err = conn.Do("DEL", redisKey)
				if err != nil {
					log.Logger.Errorf("clear redis old data failed key = %s failed, err = %v ", redisKey, err)
					return err
				}
			}
		default:
			log.Logger.Errorf("unknow RedisKeyType = %s", rule.RedisKeyType)
			return fmt.Errorf("unknow RedisKeyType")
		}
		log.Logger.Infof("clear redis old data success, key = %s  success ", redisKey)
	}
	return nil
}
