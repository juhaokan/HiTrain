## 概述
Fusion实现将mysql数据实时同步到kafka或者redis。Fusion模拟成mysql的从节点，实时解析mysqlbinlog，将解析后的数据按照一定的存储格式写入kafka或者redis。
同步到kafka：将mysql中表数据变化，格式化写入kafka。
同步到redis：需要同步mysql中哪些表，表中哪些column，同步到redis中的存储的数据类型，同步到redis中的key名字格式等都可以在元数据库表中自定义。

## 手动进行表级全量同步
提供http接口，当mysql和redis数据不同步时用户可调用此接口进行手动全量同步。当收到请求后首先暂停binlog监听流程，开启请求的数据表的全量同步，全量同步完成后从原来暂停前的位置开始监听binlog流程。
curl -X POST -d  '{"tables":"lxp1.lxp1,lxp.order_info"}' http://ip:8081/fusion/datasync

## 动态增删表
当业务需要新增加或者删除表同步，直接在配置信息表中增加或删除即可，Fusion网元监听到规则信息变化，更新到内存，实现动态增删表同步。当监听到新增或删除规则时，Fusion首先暂停当前的数据同步，如果是新增表同步并且需要全量同步则进行全量同步，如果是删除表同步则不再进行此表的同步（之前已经同步的数据不会删除，过期后会自动删除），然后从暂停位置开始增量同步。
向规则表中添加一条operation字段为3的数据，则动态增加表同步；operation字段为4，则动态删除表同步；

## mysql 主从切换后高可用
mysql采用MHA做高可用方案，fusion配置文件中配置主从地址，默认是做为mysql主节点的从，当主从切换后，Fusion自动切到新主上，从对应的位置开始同步，不会丢失数据。

## kafka中消息格式
数据格式：
{
	"sid": 20,
	"dbname": "lxp",
	"tbname": "lxp",
	"filename": "mysql-bin.000011",
	"position": 1549,
	"event": "update",
	"columns": [{
		"id": 1,
		"name": "lxp"               //update之后的
	}, {
		"id": 1,
		"name": "aa"                  //update之前的
	}, {
		"id": 2,
		"name": "lxp"            //update之后的
	}, {
		"id": 2,
		"name": "aa"             //update之前的
	}]

}

Xml格式如下：
<?xml version="1.0" encoding="UTF-8"?>
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
<columns >
          <column name="id">1</column>
          <column name="name">aa</column>
      </columns >

      <columns>
          <column name="id">2</column>
          <column name="name">lxp</column>
      </columns>
      <columns >
          <column name="id">2</column>
          <column name="name">aa</column>
      </columns >
  </content>

