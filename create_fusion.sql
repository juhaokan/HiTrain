CREATE DATABASE IF NOT EXISTS fusion DEFAULT CHARACTER SET utf8;
USE fusion;

create table if not exists `table_rule_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `sid` int(11) NOT NULL  COMMENT 'mysql2redis 网元标示,不同业务sid不同',
  `db` varchar(32) NOT NULL DEFAULT '' COMMENT '要同步的表所属库名',
  `tb` varchar(32) NOT NULL DEFAULT '' COMMENT '要同步的表名',
  `refresh_type` int(4) NOT NULL DEFAULT 1 COMMENT 'myqsl数据变化后是实时更新到redis还是从redis中删除,1代表更新,0代表删除',
  `is_dump` int(4) NOT NULL DEFAULT 0 COMMENT '首次启动时是否需要全量同步,1代表是,0代表不是',
  `is_compress` int(4) NOT NULL DEFAULT 0 COMMENT '存到redis中的数据是否需要做gzip压缩,0代表不是,1代表只做gzip压缩,2代表先gzip后base64编码',
  `compress_level` int(4) NOT NULL DEFAULT -1 COMMENT 'gzip压缩率，取值1-9 and -1，默认-1',
  `compress_columns` varchar(1024) NOT NULL DEFAULT '' COMMENT '当数据类型为hash时，指定哪些column需要压缩，column之间用逗号分隔',
  `expire_time` int(11) NOT NULL DEFAULT 3600 COMMENT '同步到redis的过期时间，单位秒，默认3600',
  `expire_range` int(11) NOT NULL DEFAULT 0 COMMENT '同步到redis的过期时间波动范围百分比，默认0，不波动，取值0---50',
  `is_full_column` int(4) NOT NULL DEFAULT 1 COMMENT '是否是同步表中所有字段,1代表是,0代表不是',
  `replic_columns` varchar(1024) NOT NULL DEFAULT '' COMMENT '如果不是同步表中所有字段，要同步的表中字段,中间用逗号隔开',
  `redis_key_prefix` varchar(256) NOT NULL DEFAULT '' COMMENT '同步到redis的数据存储的key前缀',
  `redis_key_columns` varchar(1024) NOT NULL DEFAULT '' COMMENT '同步到redis中的key格式以什么维度存储,多个column中间用逗号隔开,key格式redis_key_prefix:column1:column2',
  `redis_key_type` varchar(16) NOT NULL DEFAULT '' COMMENT '同步到redis的数据类型，string、hash、set、zset',
  `redis_zset_score_column` varchar(64) NOT NULL DEFAULT '' COMMENT '同步到redis的数据类型如果是zset，需要依据一个column作为score',
  `filter_rule` varchar(512) NOT NULL DEFAULT '' COMMENT '表中符合此条件的数据同步到redis，默认空字符串表示全量同步，此规则只支持某整型或者字符串类型的column等于某个值的过滤条件，多个column之间用逗号隔开，如column1=hello,column2=world',
  `operation` int(2) NOT NULL DEFAULT 0 COMMENT '规则修改: 0 代表初始化状态, 1 代表添加规则, 2 代表删除规则',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP PROCEDURE IF EXISTS AddColumnUnlessExists;
DELIMITER $$
CREATE PROCEDURE AddColumnUnlessExists(
    IN dbName TINYTEXT,
    IN tableName TINYTEXT,
    IN fieldName TINYTEXT,
    IN fieldDef TEXT)
BEGIN
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS
        WHERE column_name=fieldName
        AND table_name=tableName
        AND table_schema=dbName
        )
    THEN
        SET @ddl=CONCAT('ALTER TABLE ',dbName,'.',tableName,
            ' ADD COLUMN ',fieldName,' ',fieldDef);
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
    END IF;

END$$
DELIMITER ;

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'kafka_key_columns', "varchar(1024) NOT NULL DEFAULT '' COMMENT '针对多partition场景,同步到kafka中的key格式以什么维度存储,多个column中间用逗号隔开如\"column1,column2\";kafka中key格式column1:column2'");

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'kafka_topic_rule', "varchar(1024) NOT NULL DEFAULT '' COMMENT '针对多topic场景,根据column不同的值，写到不同的topic，格式{\"column_name\":\"category_id\",\"topic_rule\":{\"1008\":\"topic1\",\"1009\":\"topic2\"}}'");

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'delay_del', "int NOT NULL DEFAULT 0 COMMENT '针对delete操作，是否延迟删除redis对应的key，0不延迟，1延迟'");

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'delay_expire_time', "int NOT NULL DEFAULT 3600 COMMENT '针对delete操作, 延迟多久删除即过期redis对应的key，单位秒，只针对string和hash类型的key'");

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'kafka_is_full_column', "int(4) NOT NULL DEFAULT 1 COMMENT '表变更是否是同步表中所有字段到kafka,1代表是,0代表不是'");

CALL AddColumnUnlessExists('fusion', 'table_rule_info', 'kafka_replic_columns', "varchar(1024) NOT NULL DEFAULT '' COMMENT '表变更,指定需要同步到kafk表中的column,中间用逗号隔开'");

DROP PROCEDURE IF EXISTS AddColumnUnlessExists;


CREATE TABLE if not exists  `working_info` (
  `sid` int(11) NOT NULL COMMENT 'fusion 进程标识',
  `mysql_addr` varchar(128) NOT NULL DEFAULT '""' COMMENT '同步的mysql地址',
  `file_name` varchar(128) NOT NULL DEFAULT '"""' COMMENT '解析的文件名',
  `position` int(11) NOT NULL DEFAULT '0' COMMENT '解析到的位置',
  `timestamp` int(11) NOT NULL DEFAULT '0' COMMENT '事件产生的时间',
  PRIMARY KEY (`sid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE if not exists `mysql_pos_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `sid` int(11) NOT NULL COMMENT 'mysql2redis 网元标示,不同业务sid不同',
  `master_mysql_addr` varchar(128) NOT NULL DEFAULT '' COMMENT 'fusion监听的mysql地址',
  `master_binlog_file_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'fusion监听的mysql的binlog文件名',
  `master_binlog_position` int(11) NOT NULL DEFAULT '0' COMMENT 'fusion监听的mysql的binlog的pos位置',
  `slave_mysql_addr` varchar(128) NOT NULL DEFAULT '' COMMENT 'mysql从节点的地址',
  `slave_binlog_file_name` varchar(128) NOT NULL DEFAULT '' COMMENT 'mysql从节点的binlog文件名',
  `slave_binlog_position` int(11) NOT NULL DEFAULT '0' COMMENT 'mysql从节点的binlog的pos位置',
  `insert_timestamp` int(11) NOT NULL DEFAULT '0' COMMENT '此条数据写入的时间戳',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

commit;

