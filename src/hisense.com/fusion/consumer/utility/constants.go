package utility

// ConsumerData 发送到consumer 的数据结构
var ConsumerData []ConsumerDataStruct

// ConsumerDataChan 发送数据的通道
var ConsumerDataChan = make(chan []ConsumerDataStruct, 1000)

// PosChan working_info 表信息
var PosChan = make(chan WorkingInfo, 100)

// KeepPos 是否保存binlog 位置
var KeepPos = false
