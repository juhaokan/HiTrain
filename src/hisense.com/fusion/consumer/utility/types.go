package utility

// ColumnData Column Data Struct
type ColumnData struct {
	Name  string
	Type  int
	Value interface{}
}

// ConsumerDataStruct 发送到redis consumer 中的数据结构
type ConsumerDataStruct struct {
	Action           string
	BinlogFile       string
	BinlogPos        uint32
	EventTimestamp   uint32
	Schema           string
	Table            string
	Query            string
	ColumnData       [][]ColumnData
	ColumnDataBefore [][]ColumnData
}

// PosInfo 持久化的mysql pos信息
type PosInfo struct {
	BinlogFile     string `json:"binlogfile"`
	BinlogPos      uint32 `json:"binlogpos"`
	EventTimestamp uint32 `json:"eventtimestamp"`
}

// WorkingInfo 保存解析
type WorkingInfo struct {
	SID       int    `db:"sid"`
	MysqlAddr string `db:"mysql_addr"`
	FileName  string `db:"file_name"`
	Position  uint32 `db:"position"`
	Timestamp uint32 `db:"timestamp"`
}
