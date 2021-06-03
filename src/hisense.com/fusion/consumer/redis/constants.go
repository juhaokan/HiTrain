package redis

var (
	// RefreshTypeDel redis 操作类型为删除key
	RefreshTypeDel = 0
	// RefreshTypeUpdate redis 操作类型为更新key
	RefreshTypeUpdate = 1
	// RedisKeyTypeList 存储到redis 为list 类型, 目前暂不支持
	RedisKeyTypeList = "list"
	// RedisKeyTypeString 存储到redis 为string 类型
	RedisKeyTypeString = "string"
	// RedisKeyTypeSet 存储到redis 为 set 类型
	RedisKeyTypeSet = "set"
	// RedisKeyTypeZset 存储到redis 为 zset 类型
	RedisKeyTypeZset = "zset"
	// RedisKeyTypeHash 存储到redis 为 hash 类型
	RedisKeyTypeHash = "hash"
)

var (
	// OnlyGzip 只进行gzip压缩
	OnlyGzip = 1
	// GzipBase64 先gzip压缩，压缩后base64编码
	GzipBase64 = 2
	// Snappy 只snappy压缩
	Snappy = 3
	// SnappyBase64 先snappy压缩，压缩后base64编码
	SnappyBase64 = 4
)

var posExpireTime = 15 * 24 * 3600
