package dataparser

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"hisense.com/fusion/config"
	"hisense.com/fusion/consumer/utility"
	log "hisense.com/fusion/logger"
	"hisense.com/fusion/ruleparser"

	"github.com/siddontang/go-mysql/canal"
	//"github.com/siddontang/go-log/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

var (
	// XIDCommit 是否有XID 提交， 增量解析部分有XID event
	XIDCommit      = false
	maxRowAffected = 7

	// 若匹配到table为_table_new则是pt-online进行表表更
	ptOnlineFlag = 0
	ddlTableNew  = ""
)

// DDLTable 用来存放ddl变化的表，元素为db.tb
var DDLTable []string

// MyEventHandler 事件处理
type MyEventHandler struct {
	canal.DummyEventHandler
}

// NewConsumerData 重置 consumer 数据
func NewConsumerData() {
	data := new([]utility.ConsumerDataStruct)
	utility.ConsumerData = *data
}

// GetFilterTables 为开源同步工具canal 收集需要同步的表信息， 目前暂不支持配置exclude tables
func GetFilterTables() (map[string][]string, []string, []string) {
	dumpTables, includeTables, excludeTables := ruleparser.GetCanalFilterTables()
	log.Logger.Infof("dump tables = %v, include tables = %v, exclude tables = %v ", dumpTables, includeTables, excludeTables)
	return dumpTables, includeTables, excludeTables
}

// OnXID 填充RedisDataMap Binlog File 与binlog Pos 字段。将消息发送给 消费者（redis 数据写入）
func (h *MyEventHandler) OnXID(pos mysql.Position) error {
	log.Logger.Infof("on XID EVENT, pos = %s,  XIDCommit=%t", pos, XIDCommit)
	if XIDCommit {
		for index := range utility.ConsumerData {
			utility.ConsumerData[index].BinlogFile = pos.Name
			utility.ConsumerData[index].BinlogPos = pos.Pos
		}
		sendData2Consumer()
	} else {
		if utility.KeepPos {
			log.Logger.Infof("keep pos, binlogFile = %s, binlogPos = %d", pos.Name, pos.Pos)
			//redis.SavePos(cfg.SID, pos.Name, pos.Pos, uint32(time.Now().Unix()))
			workingInfo := utility.WorkingInfo{
				SID:       config.SID,
				FileName:  pos.Name,
				Position:  pos.Pos,
				Timestamp: uint32(time.Now().Unix()),
			}
			utility.PosChan <- workingInfo
			utility.KeepPos = false
		}
	}
	return nil
}

func constructColumnData(row []interface{}, columns []schema.TableColumn, isBefore bool, redisData *utility.ConsumerDataStruct) {
	data := []utility.ColumnData{}
	// 解决20201223线上出现base_vod basic_media增加3个字段，96个变为99个，这里columns是99个，row是96个，panic
	row_len := len(row)
	for _idx, column := range columns {
		if _idx >= row_len {
			log.Logger.Errorf("constructColumnData [row len = %d] [columns len = %d] not equal!", row_len, len(columns))
			break
		}
		log.Logger.Debugf("idx[%d] %s = %v, isBefore = %v", _idx, column.Name, row[_idx], isBefore)
		data = append(data, utility.ColumnData{Type: column.Type, Value: row[_idx], Name: column.Name})
	}
	if isBefore {
		redisData.ColumnDataBefore = append(redisData.ColumnDataBefore, data)
	} else {
		redisData.ColumnData = append(redisData.ColumnData, data)
	}
}

func sendData2Consumer() {
	log.Logger.Debugf("on Row ConsumerData, data = %+v ", utility.ConsumerData)
	utility.ConsumerDataChan <- utility.ConsumerData
	NewConsumerData()
	XIDCommit = false
}

// OnRow 解析出来的binlog 事件结构，组装成数据发送给redisconsumer
func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	log.Logger.Debugf("get canal Rows Event is %+v", e)
	commit := false
	// e.Header == nil 说明这次数据是全量数据，可以直接提交，
	if e.Header == nil {
		commit = true
	} else {
		XIDCommit = true
	}
	log.Logger.Infof("action = %s, table = %s, row len = %d", e.Action, e.Table.Name, len(e.Rows))
	log.Logger.Debugf("action = %s, table = %s, row len = %d, rows= %v", e.Action, e.Table.Name, len(e.Rows), e.Rows)
	data := new(utility.ConsumerDataStruct)
	data.Action = e.Action
	for idx, row := range e.Rows {
		if e.Action == canal.InsertAction || e.Action == canal.DeleteAction {
			constructColumnData(row, e.Table.Columns, false, data)
		}
		if e.Action == canal.UpdateAction {
			if idx%2 == 1 {
				constructColumnData(row, e.Table.Columns, false, data)
			} else {
				constructColumnData(row, e.Table.Columns, true, data)
			}
		}
	}
	log.Logger.Infof("get redis consumer data len = %d, commit=%t", len(data.ColumnData), commit)
	if e.Table != nil {
		log.Logger.Infof("schema = %s, table =%s ", e.Table.Schema, e.Table.Name)
		data.Schema = e.Table.Schema
		data.Table = e.Table.Name
	}

	if commit || len(utility.ConsumerData) > maxRowAffected {
		//sendData2Consumer() 如果直接发送，第一个包是空包
		utility.ConsumerData = append(utility.ConsumerData, *data)
                sendData2Consumer()
	} else {
		data.EventTimestamp = e.Header.Timestamp
		utility.ConsumerData = append(utility.ConsumerData, *data)
	}
	return nil
}

func (h *MyEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	// 这里的schema是指当前的ddl语句在哪个schema下执行的，并不一定是这个schema下面的表变化
	// 需要将这里的pos持久化
	if config.ConfigInfo.AppConfig.ISSupportDDL == 0 {
		return nil
	}
	log.Logger.Infof("OnDDL get ddl nextPos is %+v, queryEvent is [SlaveProxyID: %d, ExecutionTime: %d, ErrorCode: %d, Schema: %s, Query: %s]",
		nextPos, queryEvent.SlaveProxyID, queryEvent.ExecutionTime, queryEvent.ErrorCode, queryEvent.Schema, queryEvent.Query)

	// 只处理fusion监听的表
	if len(DDLTable) == 0 {
		log.Logger.Infof("OnDDL this ddl is not change on fusion includeTables, ignore!")
		return nil
	}

	db := strings.Split(DDLTable[0], ".")[0]
	strQuery := string(queryEvent.Query)

	// 截取query的第一个单词
	index := strings.Index(strQuery, " ")
	DDLType := strQuery[0:index]
	// 只处理alter类型的ddl。truncate有问题，table change中无法获取到table名
	if DDLType != "ALTER" && DDLType != "alter" {
		log.Logger.Warnf("OnDDL type: %s, not alter, fusion not execute it, ignore!", DDLType)
		ptOnlineFlag = 0
		ddlTableNew = ""
		DDLTable = []string{}
		return nil
	}
	var newDDL string
	if ptOnlineFlag == 0 {
		newDDL = strQuery
		log.Logger.Infof("OnDDL not pt-online-schema-change! can exec it directly! ddl: %s", newDDL)
	} else {
		newDDL = strings.Replace(strQuery, ddlTableNew, strings.Split(DDLTable[0], ".")[1], 1)
		log.Logger.Infof("OnDDL is pt-online-schema-change! transfor ddl to: %s", newDDL)
	}

	ptOnlineFlag = 0
	ddlTableNew = ""
	DDLTable = []string{}

	data := new(utility.ConsumerDataStruct)
	data.Action = "query"
	data.Schema = db
	data.Query = newDDL
	data.BinlogFile = nextPos.Name
	data.BinlogPos = nextPos.Pos
	utility.ConsumerData = append(utility.ConsumerData, *data)
	sendData2Consumer()

	workingInfo := utility.WorkingInfo{
		SID:       config.SID,
		FileName:  nextPos.Name,
		Position:  nextPos.Pos,
		Timestamp: uint32(time.Now().Unix()),
	}
	utility.PosChan <- workingInfo

	return nil
}

// ddl的时候会先收到OnTableChanged事件，再收到上面的OnDDL事件，当一个ddl语句涉及到多个表时，会收到多个OnTableChanged事件，一个OnDDL事件
func (h *MyEventHandler) OnTableChanged(schema string, table string) error {
	if config.ConfigInfo.AppConfig.ISSupportDDL == 0 {
		return nil
	}
	log.Logger.Infof("OnTableChanged schema: %s, table: %s", schema, table)
	key := fmt.Sprintf("%s.%s", schema, table)
	_, includeTables, _ := GetFilterTables()
	for _, DBTB := range includeTables {
		if key == DBTB {
			DDLTable = append(DDLTable, key)
			break
		} else {
			db := strings.Split(DBTB, ".")[0]
			tb := strings.Split(DBTB, ".")[1]
			pat := fmt.Sprintf("%s._*%s_new", db, tb)
			if ok, _ := regexp.Match(pat, []byte(key)); ok {
				log.Logger.Infof("OnTableChanged schema: %s, table: %s is pt-online-schema-change!", db, tb)
				DDLTable = append(DDLTable, DBTB)
				ptOnlineFlag = 1
				ddlTableNew = table
				break
			}
		}

	}

	// time.Sleep(time.Second)
	return nil
}

// String 返回mysql event handler 的名称
func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}
