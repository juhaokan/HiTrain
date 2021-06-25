# 使用说明
linux环境运行

## 部署建议
* mysql 的binlog格式需要为ROW模式
* 如果使用的数据库版本为mysql5.7，需要配置show_compatibility_56=ON
* 若同步到kafka，创建的topic需要为单partition，以保证顺序

## 编译、部署
下载源码后，执行如下命令进行编译
```bash
cd scripts
sh build.sh
```

编译完成后会产生bin目录，里面包含编译后的可执行文件hitrain、配置文件、启动脚本。

或者说不需要编译，直接下载可执行文件即可。

* 首先使用create_train.sql创建元数据库，然后将要同步的信息配置到table_rule_info表
* 根据实际情况修改配置文件后，执行启动脚本（可自行修改启动参数）进行部署。



