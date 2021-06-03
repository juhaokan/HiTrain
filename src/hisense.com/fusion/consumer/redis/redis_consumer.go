package redis

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/redisutil"
	"hisense.com/fusion/ruleparser"
)

func trimKeysString(str string, split string) []string {
	var v []string
	for _, k := range strings.Split(str, split) {
		v = append(v, strings.TrimSpace(k))
	}
	log.Logger.Infof("trim key get value %v ", v)
	return v
}

func addValue(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		return fmt.Sprintf("%s", v.([]byte))
	}
	return fmt.Sprintf("%v", v)
}

// addValueForString 解决bug HISTORAGE-52
func addValueForString(v interface{}) interface{} {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		return fmt.Sprintf("%s", v.([]byte))
	}
	return v
}

func getColumnKey(keys []string, data []utility.ColumnData) string {
	sep := ""
	retKey := ""
	for _, _key := range keys {
		for _, v := range data {
			if _key == v.Name {
				retKey = fmt.Sprintf("%s%s%s", retKey, sep, addValue(v.Value))
				sep = ":"
			}
		}
	}
	log.Logger.Infof("get column key is %s ", retKey)
	return retKey
}

func isReplicColumnMatch(data []utility.ColumnData, key string) (bool, utility.ColumnData) {
	for _, v := range data {
		if key == v.Name {
			return true, v
		}
	}
	return false, utility.ColumnData{}
}

func getColumnStringValue(rule ruleparser.Rule, data []utility.ColumnData) interface{} {
	columnMap := map[string]interface{}{}
	if rule.ISFullColumn > 0 {
		for _, v := range data {
			columnMap[v.Name] = addValueForString(v.Value)
		}
	} else {
		replicColumns := trimKeysString(rule.ReplicColumns, ",")
		for _, column := range replicColumns {
			ok, v := isReplicColumnMatch(data, column)
			if ok {
				columnMap[v.Name] = addValueForString(v.Value)
			}
		}
	}

	value, _ := json.Marshal(columnMap)
	if rule.ISCompress > 0 {
		v, err := gzipEncode(value, rule.ISCompress, rule.CompressLevel)
		if err != nil {
			log.Logger.Errorf("gzip data failed ,err = %v ", err)
			return "Error GZIP Data"
		}
		return v
	}
	return value
}

func getColumnScoreValue(rule ruleparser.Rule, data []utility.ColumnData) interface{} {
	col := strings.TrimSpace(rule.ZSetColumn)
	ok, v := isReplicColumnMatch(data, col)
	if ok {
		return v.Value
	}
	return nil
}

func isColumnCompress(key string, compressColumns string) bool {
	if len(compressColumns) == 0 {
		return false
	}
	l := trimKeysString(compressColumns, ",")
	for _, _key := range l {
		if key == _key {
			return true
		}
	}
	return false
}

func getColumnHashValue(rule ruleparser.Rule, data []utility.ColumnData) map[string]interface{} {
	value := make(map[string]interface{})
	if rule.ISFullColumn > 0 {
		for _, _v := range data {
			if rule.ISCompress > 0 && isColumnCompress(_v.Name, rule.CompressColumns) {
				v, err := gzipEncode([]byte(fmt.Sprintf("%s", addValue(_v.Value))), rule.ISCompress, rule.CompressLevel)
				if err != nil {
					log.Logger.Errorf("gzip data failed ,err = %v ", err)
					return value
				}
				value[_v.Name] = v
			} else {
				value[_v.Name] = fmt.Sprintf("%s", addValue(_v.Value))
			}
		}
	} else {
		replicColumns := trimKeysString(rule.ReplicColumns, ",")
		for _, column := range replicColumns {
			ok, _v := isReplicColumnMatch(data, column)
			if ok {
				if rule.ISCompress > 0 && isColumnCompress(column, rule.CompressColumns) {
					v, err := gzipEncode([]byte(fmt.Sprintf("%s", addValue(_v.Value))), rule.ISCompress, rule.CompressLevel)
					if err != nil {
						log.Logger.Errorf("gzip data failed ,err = %v ", err)
						return value
					}
					value[column] = v
				} else {
					value[column] = fmt.Sprintf("%s", addValue(_v.Value))
				}
			}
		}
	}
	return value
}

func getExpiredTime(rule ruleparser.Rule) int {
	expireTime := rule.ExpireTime
	if expireTime == 0 {
		expireTime = 24 * 3600
	}
	if rule.ExpireRange > 0 && rule.ExpireRange < 100 {
		rand.Seed(int64(time.Now().UnixNano()))
		min := (expireTime * (100 - rule.ExpireRange)) / 100
		//max := (expireTime * (100 + rule.ExpireRange))/100
		r := (2 * expireTime * (rule.ExpireRange)) / 100
		expireTime = min + rand.Intn(r)
	} else {
		log.Logger.Warnf("ExpireRange should between 0 and 100, ExpireRange = %d, expireTime = %d", rule.ExpireRange, expireTime)
	}
	return expireTime
}

func dataToRedis(wg *sync.WaitGroup, rule ruleparser.Rule, data utility.ConsumerDataStruct) error {
	defer wg.Done()
	for {
		if data.Action == canal.UpdateAction {
			if err := clearRedisOldData(rule, data); err != nil {
				log.Logger.Errorf("clearRedisOldData handle failed ,err = %v", err)
				time.Sleep(time.Second)
				continue
			}
		}
		if err := handleRedisType(rule, data); err != nil {
			log.Logger.Errorf("handleRedisType handle failed ,err = %v", err)
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
}

// SavePos save mysql binlog position
func SavePos(sid int, binlogFile string, binlogPos uint32, timestamp uint32) error {
	if len(binlogFile) == 0 {
		return nil
	}
	conn := redisutil.Pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("%s:%s:%d:pos", config.Project, config.NEName, sid)

	posInfo := utility.PosInfo{
		BinlogFile:     binlogFile,
		BinlogPos:      binlogPos,
		EventTimestamp: timestamp}

	posStr, err := json.Marshal(posInfo)
	if err != nil {
		log.Logger.Errorf("save pos Marshal error, err = %v", err)
		return err
	}

	_, err = conn.Do("SETEX", key, posExpireTime, posStr)
	if err != nil {
		log.Logger.Errorf("save pos error, err = %v", err)
		return err
	}

	log.Logger.Infof("save binlog file = %s, binlog pos = %d to redis success ", binlogFile, binlogPos)
	return nil
}

//GetPos 获取redis 持久化的位置
func GetPos(sid int) (string, int) {
	conn := redisutil.Pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("%s:%s:%d:pos", config.Project, config.NEName, sid)

	data, err := conn.Do("GET", key)
	if err != nil {
		log.Logger.Errorf("get sid = %d pos failed, err = %v", sid, err)
		return "", 0
	}
	if data == nil {
		log.Logger.Infof("get sid = %d pos nil", sid)
		return "", 0
	}

	var posInfo utility.PosInfo
	err = json.Unmarshal(data.([]uint8), &posInfo)
	if err != nil {
		log.Logger.Errorf("get sid = %d pos Unmarshal err:%s\n", sid, err)
		return "", 0
	}

	return posInfo.BinlogFile, int(posInfo.BinlogPos)
}

func fusionData(rules []ruleparser.Rule, data utility.ConsumerDataStruct) error {
	var wg sync.WaitGroup
	for _, rule := range rules {
		wg.Add(1)
		go dataToRedis(&wg, rule, data)
	}
	wg.Wait()
	return nil
}

func save(sid int, binFile string, binPos uint32, stamp uint32) error {
	if len(binFile) == 0 {
		return nil
	}
	workingInfo := utility.WorkingInfo{
		SID:       sid,
		FileName:  binFile,
		Position:  binPos,
		Timestamp: stamp,
	}
	utility.PosChan <- workingInfo
	return nil
}

// FusionProcessing 处理数据，写入redis
func FusionProcessing(redisData []utility.ConsumerDataStruct) error {
	for _, data := range redisData {
		key := fmt.Sprintf("%s.%s", data.Schema, data.Table)
		rules := ruleparser.GetRules(key)
		if len(rules) == 0 || rules == nil {
			log.Logger.Errorf("key = %s not exists in rules, rule = %+v ", key, ruleparser.FusionRules)
			return fmt.Errorf("key not exits")
		}
		fusionData(rules, data)
	}

	if len(redisData) == 0 {
		return nil
	}
	return save(config.SID, redisData[0].BinlogFile, redisData[0].BinlogPos, redisData[0].EventTimestamp)
}
