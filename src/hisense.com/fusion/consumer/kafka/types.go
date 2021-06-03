package kafka

import (
	"encoding/xml"
)

// JSONKafkaMsg data format saved in kafka
type JSONKafkaMsg struct {
	SID       int                      `json:"sid"`
	DBName    string                   `json:"dbname"`
	TBName    string                   `json:"tbname"`
	FileName  string                   `json:"filename"`
	Position  uint32                   `json:"position"`
	EventTime uint32                   `json:"event_time"`
	Event     string                   `json:"event"`
	Columns   []map[string]interface{} `json:"columns"`
}

// JSONKafkaMsg data format saved in kafka (add query field for ddl)
type JSONKafkaMsgQuery struct {
	SID       int                      `json:"sid"`
	DBName    string                   `json:"dbname"`
	TBName    string                   `json:"tbname"`
	FileName  string                   `json:"filename"`
	Position  uint32                   `json:"position"`
	EventTime uint32                   `json:"event_time"`
	Event     string                   `json:"event"`
	Query     string                   `json:"query"`
	Columns   []map[string]interface{} `json:"columns"`
}

// XMLkafkaMsg data format saved in kafka
type XMLkafkaMsg struct {
	XMLName   xml.Name  `xml:"contenet"`
	SID       int       `xml:"sid"`
	DBName    string    `xml:"dbname"`
	TBName    string    `xml:"tbname"`
	FileName  string    `xml:"filename"`
	Position  uint32    `xml:"position"`
	EventTime uint32    `xml:"event_time"`
	Event     string    `xml:"event"`
	Columns   []Columns `xml:"columns"`
}

// Columns row information
type Columns struct {
	XMLName xml.Name `xml:"columns"`
	Column  []Column `xml:"column"`
}

//Column column info
type Column struct {
	Name  string      `xml:"name,attr"`
	Value interface{} `xml:",chardata"`
}
