package config

var (
	// Project  项目名称
	Project = "historage"
	// NEName 网元名称
	NEName = "fusiondataprocessing"
)

// ConfigFile 配置文件绝对地址
var ConfigFile string

// LogPath 日志文件地址
var LogPath string

// LogLevel 日志等级
var LogLevel string

// SID fusion 标识
var SID int

// TargetMysqlAddr 连接的mysql地址
var TargetMysqlAddr string

// ConfigInfo 配置信息
var ConfigInfo *FusionConfig
