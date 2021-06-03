package kafka

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"reflect"
	"strings"
	"time"

	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/ruleparser"

	"github.com/Shopify/sarama"
	"github.com/siddontang/go-mysql/canal"
)

var syncProducer sarama.SyncProducer

// GetPos 从zookeeper 中获取需要同步的位置
func GetPos(sid int) (string, int) {
	return "", 0
}

// ClearPos 清掉同步的信息
func ClearPos(sid int) error {
	return nil
}
func addValue(v interface{}) interface{} {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		return fmt.Sprintf("%s", v.([]byte))
	}
	return v
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

func toMap(data utility.ConsumerDataStruct) []map[string]interface{} {
	rsts := []map[string]interface{}{}
	for idx, cols := range data.ColumnData {
		rst := make(map[string]interface{})
		for _, col := range cols {
			rst[col.Name] = addValue(col.Value)
		}
		rsts = append(rsts, rst)
		if data.Action == canal.UpdateAction {
			beforeCols := data.ColumnDataBefore[idx]
			rst := make(map[string]interface{})
			for _, beforeCol := range beforeCols {
				rst[beforeCol.Name] = addValue(beforeCol.Value)

			}
			rsts = append(rsts, rst)
		}
	}
	return rsts
}

// 针对json格式组装columns
func constructColumnsForJson(replicColumns []string, cols []utility.ColumnData) map[string]interface{} {
	rst := make(map[string]interface{})
	columnMap := make(map[string]interface{})
	for _, col := range cols {
		columnMap[col.Name] = col.Value
	}

	for _, column := range replicColumns {
		v, exists := columnMap[column]
		if exists {
			rst[column] = addValue(v)
		}
	}
	return rst
}

// 针对xml格式组装columns
func constructColumnsForXml(replicColumns []string, cols []utility.ColumnData) []Column {
	var rst []Column
	columnMap := make(map[string]interface{})
	for _, col := range cols {
		columnMap[col.Name] = col.Value
	}

	for _, column := range replicColumns {
		value, exists := columnMap[column]
		if exists {
			v := Column{
				Name:  column,
				Value: addValue(value),
			}
			rst = append(rst, v)
		}
	}
	return rst
}

// json格式需要组装的column格式
func toMapCommon(data utility.ConsumerDataStruct, rules []ruleparser.Rule) []map[string]interface{} {
	if rules == nil {
		rsts := toMap(data)
		return rsts
	}
	// 一个表可能需要配置多个规则，每条规则都必须配置要同步到kafka的columns，取第一条规则即可
	rule := rules[0]
	if rule.KafkaISFullColumn == 1 {
		rsts := toMap(data)
		return rsts
	}

	rsts := []map[string]interface{}{}
	replicColumns := trimKeysString(rule.KafkaReplicColumns, ",")
	for idx, cols := range data.ColumnData {
		rst := constructColumnsForJson(replicColumns, cols)
		rsts = append(rsts, rst)

		if data.Action == canal.UpdateAction {
			beforeCols := data.ColumnDataBefore[idx]
			rst := constructColumnsForJson(replicColumns, beforeCols)
			rsts = append(rsts, rst)
		}
	}
	return rsts
}

func toColumns(data utility.ConsumerDataStruct) []Columns {
	var rsts []Columns
	for idx, cols := range data.ColumnData {
		var rst []Column
		for _, col := range cols {
			v := Column{
				Name:  col.Name,
				Value: addValue(col.Value),
			}
			rst = append(rst, v)
		}
		rsts = append(rsts, Columns{Column: rst})
		if data.Action == canal.UpdateAction {
			beforeCols := data.ColumnDataBefore[idx]
			var rst []Column
			for _, beforeCol := range beforeCols {
				v := Column{
					Name:  beforeCol.Name,
					Value: addValue(beforeCol.Value),
				}
				rst = append(rst, v)
			}
			rsts = append(rsts, Columns{Column: rst})
		}
	}
	return rsts
}

// xml格式组装的column格式
func toColumnsCommon(data utility.ConsumerDataStruct, rules []ruleparser.Rule) []Columns {
	if rules == nil {
		rsts := toColumns(data)
		return rsts
	}
	// 一个表可能需要配置多个规则，每条规则都必须配置要同步到kafka的columns，取第一条规则即可
	rule := rules[0]
	if rule.KafkaISFullColumn == 1 {
		rsts := toColumns(data)
		return rsts
	}

	var rsts []Columns
	replicColumns := trimKeysString(rule.KafkaReplicColumns, ",")
	for idx, cols := range data.ColumnData {
		rst := constructColumnsForXml(replicColumns, cols)
		rsts = append(rsts, Columns{Column: rst})

		if data.Action == canal.UpdateAction {
			beforeCols := data.ColumnDataBefore[idx]
			rst := constructColumnsForXml(replicColumns, beforeCols)
			rsts = append(rsts, Columns{Column: rst})
		}
	}
	return rsts
}

func constructJSONFormatData(data utility.ConsumerDataStruct, rules []ruleparser.Rule) ([]byte, error) {
	var vBytes []byte
	var err error
	retData := JSONKafkaMsg{
		SID:       config.SID,
		DBName:    data.Schema,
		TBName:    data.Table,
		FileName:  data.BinlogFile,
		Position:  data.BinlogPos,
		Event:     data.Action,
		EventTime: data.EventTimestamp,
		Columns:   toMapCommon(data, rules),
	}

	if vBytes, err = json.Marshal(&retData); err != nil {
		log.Logger.Errorf("marshal data err, data = %v, err = %v", retData, err)
		return vBytes, err
	}
	return vBytes, nil

}

func constructJSONFormatDataForQuery(data utility.ConsumerDataStruct) ([]byte, error) {
	var vBytes []byte
	var err error
	retData := JSONKafkaMsgQuery{
		SID:       config.SID,
		DBName:    data.Schema,
		TBName:    data.Table,
		FileName:  data.BinlogFile,
		Position:  data.BinlogPos,
		Event:     data.Action,
		EventTime: data.EventTimestamp,
		Query:     data.Query,
		Columns:   []map[string]interface{}{},
	}

	if vBytes, err = json.Marshal(&retData); err != nil {
		log.Logger.Errorf("marshal data err, data = %v, err = %v", retData, err)
		return vBytes, err
	}
	return vBytes, nil

}

func constructXMLFormatData(data utility.ConsumerDataStruct, rules []ruleparser.Rule) ([]byte, error) {
	var vBytes []byte
	var err error
	retData := XMLkafkaMsg{
		SID:       config.SID,
		DBName:    data.Schema,
		TBName:    data.Table,
		FileName:  data.BinlogFile,
		Position:  data.BinlogPos,
		Event:     data.Action,
		EventTime: data.EventTimestamp,
		Columns:   toColumnsCommon(data, rules),
	}
	if vBytes, err = xml.Marshal(&retData); err != nil {
		log.Logger.Errorf("marshal data err, data = %v, err = %v", retData, err)
		return vBytes, err
	}
	return vBytes, nil
}

func trimKeysString(str string, split string) []string {
	var v []string
	for _, k := range strings.Split(str, split) {
		v = append(v, strings.TrimSpace(k))
	}
	log.Logger.Debugf("trim key get value %v ", v)
	return v
}

func value2String(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		return fmt.Sprintf("%s", v.([]byte))
	}
	return fmt.Sprintf("%v", v)
}

func getKafkaKey(kafkaKey string, data []utility.ColumnData) string {
	keys := trimKeysString(kafkaKey, ",")
	sep := ""
	retKey := ""
	for _, _key := range keys {
		for _, v := range data {
			if _key == v.Name {
				retKey = fmt.Sprintf("%s%s%s", retKey, sep, value2String(v.Value))
				sep = ":"
			}
		}
	}
	log.Logger.Debugf("get kafka key is %s ", retKey)
	return retKey
}

func getColumnValue(key string, data []utility.ColumnData) string {
	var retKey string
	for _, v := range data {
		if key == v.Name {
			retKey = value2String(v.Value)
		}
	}

	log.Logger.Debugf("getColumnValue is %s ", retKey)
	return retKey
}

func toMapIdx(data utility.ConsumerDataStruct, idx int) []map[string]interface{} {
	rsts := []map[string]interface{}{}
	cols := data.ColumnData[idx]
	rst := make(map[string]interface{})
	for _, col := range cols {
		rst[col.Name] = addValue(col.Value)
	}
	rsts = append(rsts, rst)
	if data.Action == canal.UpdateAction {
		beforeCols := data.ColumnDataBefore[idx]
		rst := make(map[string]interface{})
		for _, beforeCol := range beforeCols {
			rst[beforeCol.Name] = addValue(beforeCol.Value)

		}
		rsts = append(rsts, rst)
	}

	return rsts
}

func toColumnsIdx(data utility.ConsumerDataStruct, idx int) []Columns {
	var rsts []Columns
	cols := data.ColumnData[idx]
	var rst []Column
	for _, col := range cols {
		v := Column{
			Name:  col.Name,
			Value: addValue(col.Value),
		}
		rst = append(rst, v)
	}
	rsts = append(rsts, Columns{Column: rst})
	if data.Action == canal.UpdateAction {
		beforeCols := data.ColumnDataBefore[idx]
		var rst []Column
		for _, beforeCol := range beforeCols {
			v := Column{
				Name:  beforeCol.Name,
				Value: addValue(beforeCol.Value),
			}
			rst = append(rst, v)
		}
		rsts = append(rsts, Columns{Column: rst})
	}

	return rsts
}

// 针对多topic和多partiton场景 json 的column组装
func toMapIdxCommon(data utility.ConsumerDataStruct, idx int, rules []ruleparser.Rule) []map[string]interface{} {
	if rules == nil {
		rsts := toMapIdx(data, idx)
		return rsts
	}
	// 一个表可能需要配置多个规则，每条规则都必须配置要同步到kafka的columns，取第一条规则即可
	rule := rules[0]
	if rule.KafkaISFullColumn == 1 {
		rsts := toMapIdx(data, idx)
		return rsts
	}

	rsts := []map[string]interface{}{}
	cols := data.ColumnData[idx]
	replicColumns := trimKeysString(rule.KafkaReplicColumns, ",")
	rst := constructColumnsForJson(replicColumns, cols)
	rsts = append(rsts, rst)

	if data.Action == canal.UpdateAction {
		beforeCols := data.ColumnDataBefore[idx]
		rst := constructColumnsForJson(replicColumns, beforeCols)
		rsts = append(rsts, rst)
	}

	return rsts
}

// 针对多topic和多partiton场景 xml格式的column组装
func toColumnsIdxCommon(data utility.ConsumerDataStruct, idx int, rules []ruleparser.Rule) []Columns {
	if rules == nil {
		rsts := toColumnsIdx(data, idx)
		return rsts
	}
	// 一个表可能需要配置多个规则，每条规则都必须配置要同步到kafka的columns，取第一条规则即可
	rule := rules[0]
	if rule.KafkaISFullColumn == 1 {
		rsts := toColumnsIdx(data, idx)
		return rsts
	}

	var rsts []Columns
	cols := data.ColumnData[idx]
	replicColumns := trimKeysString(rule.KafkaReplicColumns, ",")
	rst := constructColumnsForXml(replicColumns, cols)
	rsts = append(rsts, Columns{Column: rst})

	if data.Action == canal.UpdateAction {
		beforeCols := data.ColumnDataBefore[idx]
		rst := constructColumnsForXml(replicColumns, beforeCols)
		rsts = append(rsts, Columns{Column: rst})
	}

	return rsts
}

func constructJSONFormatDataIdx(data utility.ConsumerDataStruct, idx int, rules []ruleparser.Rule) ([]byte, error) {
	var vBytes []byte
	var err error
	retData := JSONKafkaMsg{
		SID:       config.SID,
		DBName:    data.Schema,
		TBName:    data.Table,
		FileName:  data.BinlogFile,
		Position:  data.BinlogPos,
		Event:     data.Action,
		EventTime: data.EventTimestamp,
		Columns:   toMapIdxCommon(data, idx, rules),
	}

	if vBytes, err = json.Marshal(&retData); err != nil {
		log.Logger.Errorf("marshal data err, data = %v, err = %v", retData, err)
		return vBytes, err
	}
	return vBytes, nil

}

func constructXMLFormatDataIdx(data utility.ConsumerDataStruct, idx int, rules []ruleparser.Rule) ([]byte, error) {
	var vBytes []byte
	var err error
	retData := XMLkafkaMsg{
		SID:       config.SID,
		DBName:    data.Schema,
		TBName:    data.Table,
		FileName:  data.BinlogFile,
		Position:  data.BinlogPos,
		Event:     data.Action,
		EventTime: data.EventTimestamp,
		Columns:   toColumnsIdxCommon(data, idx, rules),
	}
	if vBytes, err = xml.Marshal(&retData); err != nil {
		log.Logger.Errorf("marshal data err, data = %v, err = %v", retData, err)
		return vBytes, err
	}
	return vBytes, nil
}

func sendToMultiTopic(cfg *config.KafkaConfig, data utility.ConsumerDataStruct, rules []ruleparser.Rule) error {
	mapKey := fmt.Sprintf("%s.%s", data.Schema, data.Table)
	dataFormat := cfg.DataFormat
	var msges []*sarama.ProducerMessage
	for idx, columnData := range data.ColumnData {
		kmsg := &sarama.ProducerMessage{}
		var msg []byte
		var err error
		switch dataFormat {
		case jsonFormat:
			msg, err = constructJSONFormatDataIdx(data, idx, rules)
		case xmlFormat:
			msg, err = constructXMLFormatDataIdx(data, idx, rules)
		default:
			return fmt.Errorf("unknown data format= %v", dataFormat)
		}
		if err != nil {
			return err
		}
		kmsg.Value = sarama.ByteEncoder(msg)

		kafkaKeyColumns, ok := ruleparser.KafkaKeyMap[mapKey]
		if ok {
			kmsg.Key = sarama.ByteEncoder([]byte(getKafkaKey(kafkaKeyColumns, columnData)))
		}
		kafkaTopicKeyColumnName, ok := ruleparser.KafkaTopicMap[mapKey]
		if ok {
			columnValue := getColumnValue(kafkaTopicKeyColumnName, columnData)
			k := fmt.Sprintf("%s.%s", mapKey, columnValue)
			topic, ok := ruleparser.KafkaTopicRuleMap[k]
			if ok {
				kmsg.Topic = topic
			} else {
				kmsg.Topic = cfg.Topic
			}
		} else {
			kmsg.Topic = cfg.Topic
		}
		log.Logger.Debugf("producer add msg to list, topic = %s, key = %s, msg = %s", kmsg.Topic, kmsg.Key, kmsg.Value)
		msges = append(msges, kmsg)
	}

	for {
		err := syncProducer.SendMessages(msges)
		if err != nil {
			log.Logger.Errorf("producer send msg failed, data info: db:%s tb:%s action:%s binlog:%s pos:%d. errinfo = %v", data.Schema, data.Table, data.Action, data.BinlogFile, data.BinlogPos, err)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	log.Logger.Infof("msg send to kafka success, msg num=%d, data info: db:%s tb:%s action:%s binlog:%s pos:%d.", len(msges), data.Schema, data.Table, data.Action, data.BinlogFile, data.BinlogPos)

	return nil
}

func getColumnRules(isSupportDefineColumn int, key string) []ruleparser.Rule {
	var rules []ruleparser.Rule
	if isSupportDefineColumn != 0 {
		rules = ruleparser.GetRules(key)
		if len(rules) == 0 || rules == nil {
			log.Logger.Warnf("key = %s not exists in rules, rule = %+v ", key, ruleparser.FusionRules)
			return nil
		}
	}
	return rules
}

// FusionProcessing 处理、封装数据，发送到kafka 中
func FusionProcessing(cfg *config.KafkaConfig, kafkaData []utility.ConsumerDataStruct) error {
	dataFormat := cfg.DataFormat
	isSupportDefineColumn := config.ConfigInfo.AppConfig.ISSupportDefineColumn
	for _, data := range kafkaData {
		mapKey := fmt.Sprintf("%s.%s", data.Schema, data.Table)
		// 20201203 支持发送到kafka中的数据自定义column的功能
		rules := getColumnRules(isSupportDefineColumn, mapKey)

		_, isMultiTopic := ruleparser.KafkaTopicMap[mapKey]
		_, isMultiPartition := ruleparser.KafkaKeyMap[mapKey]
		// query是针对ddl
		if !isMultiTopic && !isMultiPartition || data.Action == "query" {
			log.Logger.Debugf("not MultiTopic not MultiPartition.")
			kmsg := &sarama.ProducerMessage{}
			kmsg.Topic = cfg.Topic
			var msg []byte
			var err error
			switch dataFormat {
			case jsonFormat:
				if data.Action == "query" {
					msg, err = constructJSONFormatDataForQuery(data)
				} else {
					msg, err = constructJSONFormatData(data, rules)
				}

			case xmlFormat:
				msg, err = constructXMLFormatData(data, rules)
			default:
				return fmt.Errorf("unknown data format= %v", dataFormat)
			}
			if err != nil {
				return err
			}
			kmsg.Value = sarama.ByteEncoder(msg)

			log.Logger.Debugf("producer readby to send msg, topic = %s, msg = %s", cfg.Topic, kmsg.Value)
			for {
				_, _, err := syncProducer.SendMessage(kmsg)
				if err != nil {
					log.Logger.Errorf("producer send msg failed, err = %v, topic = %s", err, cfg.Topic)
					time.Sleep(time.Second)
					continue
				}
				break
			}

			log.Logger.Infof("msg send to kafka success, topic = %s, len=%d", cfg.Topic, kmsg.Value.Length())
		} else {
			sendToMultiTopic(cfg, data, rules)
		}
	}
	if len(kafkaData) == 0 {
		return nil
	}

	return save(config.SID, kafkaData[0].BinlogFile, kafkaData[0].BinlogPos, kafkaData[0].EventTimestamp)
}

// InitKafkaProducer 初始化kafka producer
func InitKafkaProducer(kafkaCFG *config.KafkaConfig) error {
	kafkaAddrs := strings.Split(kafkaCFG.BrokerList, ",")
	cfg := sarama.NewConfig()
	sarama.MaxRequestSize = int32(kafkaCFG.MaxRequestSize)
	// 等待所有broker acks ok
	//cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	cfg.Producer.MaxMessageBytes = kafkaCFG.MaxMessageBytes
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	if kafkaCFG.FlushFrequency > 0 {
		cfg.Producer.Flush.Frequency = kafkaCFG.FlushFrequency
	}
	if int(kafkaCFG.FlushMAXMessages) > 0 {
		cfg.Producer.Flush.Messages = kafkaCFG.FlushMAXMessages
	}

	producer, err := sarama.NewSyncProducer(kafkaAddrs, cfg)
	if err != nil {
		log.Logger.Errorf("init kafka producer failed, err = %v", err)
		return err

	}
	syncProducer = producer
	return nil
}
