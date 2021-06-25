## 概述
HiTrain实现将mysql数据实时同步到kafka或者redis。HiTrain模拟成mysql的从节点，实时解析mysqlbinlog，将解析后的数据按照一定的存储格式写入kafka或者redis。
* 同步到kafka：将mysql中表数据变化，格式化写入kafka。
  解决了无法实时感知数据库变化的问题。
* 同步到redis：需要同步mysql中哪些表，表中哪些column，同步到redis中的存储的数据类型，同步到redis中的key名字格式等都可以在元数据库表中自定义。
  解决了服务双写mysql和redis的问题，业务可以负责写mysql，由HiTrain写到redis，业务直接读redis使用。

## 手动进行表级全量同步
提供http接口，当mysql和redis数据不同步时用户可调用此接口进行手动全量同步。当收到请求后首先暂停binlog监听流程，开启请求的数据表的全量同步，全量同步完成后从原来暂停前的位置开始监听binlog流程。
```bash
curl -X POST -d  '{"tables":"lxp1.lxp1,lxp.order_info"}' http://ip:8081/fusion/datasync
```

## 动态增删表
当业务需要新增加或者删除表同步，直接在配置信息表中增加或删除即可，HiTrain网元监听到规则信息变化，更新到内存，实现动态增删表同步。当监听到新增或删除规则时，HiTrain首先暂停当前的数据同步，如果是新增表同步并且需要全量同步则进行全量同步，如果是删除表同步则不再进行此表的同步（之前已经同步的数据不会删除，过期后会自动删除），然后从暂停位置开始增量同步。

向规则表中添加一条operation字段为3的数据，则动态增加表同步；operation字段为4，则动态删除表同步；

## mysql 主从切换后高可用
mysql采用MHA做高可用方案，hitrain配置文件中配置主从地址，默认是做为mysql主节点的从，当主从切换后，HiTrain自动切到新主上，从对应的位置开始同步，不会丢失数据。

## kafka中消息格式
json数据格式：
```bash
{
	"sid": 20,
	"dbname": "school",
	"tbname": "class",
	"filename": "mysql-bin.000011",
	"position": 1549,
	"event": "update",
	"columns": [{
		"id": 1,
		"name": "lxp"
	}, {
		"id": 1,
		"name": "lbg"
	}, {
		"id": 2,
		"name": "hxf"
	}, {
		"id": 2,
		"name": "hxj"
	}]

}
```

xml数据格式
```bash
<?xml version="1.0" encoding="utf-8"?>

<content> 
  <dbname>lxp</dbname>  
  <tbname>lxp</tbname>  
  <filename>mysql-bin.000011</filename>  
  <position>1549</position>  
  <event>update</event>  
  <columns> 
    <column name="id">1</column>  
    <column name="name">lxp</column> 
  </columns>  
  <columns> 
    <column name="id">1</column>  
    <column name="name">lbg</column> 
  </columns>  
  <columns> 
    <column name="id">2</column>  
    <column name="name">hxf</column> 
  </columns>  
  <columns> 
    <column name="id">2</column>  
    <column name="name">hxj</column> 
  </columns> 
</content>
```

## alter语句同步
支持常用的ddl语句的同步（alter语句，包括直接使用alter和使用pt-online-schema-change工具）。

## 指定表的column数据同步到kafka
通过配置文件的开关和规则表中的kafka_is_full_column和kafka_replic_columns字段控制。

* `kafka_is_full_column` int(4) NOT NULL DEFAULT '1' COMMENT '表变更是否是同步表中所有字段到kafka,1代表是,0代表不是',
* `kafka_replic_columns` varchar(1024) NOT NULL DEFAULT '' COMMENT '表变更,指定需要同步到kafk表中的column,中间用逗号隔开',


