# HiTrain
实现mysql replication功能，HiTrain模拟成mysql的从节点，实时解析mysqlbinlog，将解析后的数据按照一定的存储格式写入kafka或者redis。
* 同步到kafka：将mysql中表数据变化，格式化写入kafka。
  解决了无法实时感知数据库变化的问题。
* 同步到redis：需要同步mysql中哪些表，表中哪些column，同步到redis中的存储的数据类型，同步到redis中的key名字格式等都可以在元数据库表中自定义。
  解决了服务双写mysql和redis的问题，业务可以负责写mysql，由HiTrain写到redis，业务直接读redis使用。

## 功能说明
[中文](doc/sds.md)

## 使用说明
[中文](doc/releasenotes.md)

## Architecture
![architecture](doc/HiTrain.png)

## Company
[聚好看](https://www.juhaokan.org/#/home)（聚好看科技股份有限公司）是海信集团下属的家庭互联网AI公司，成立于2016年7月。聚好看科技致力于成为领先的家庭互联网AI公司，为全球亿万家庭提供场景化服务。
2018--2020连续3年被评为“中国独角兽企业”，成为行业里程碑。

## Authors
* [@juhaokan](https://github.com/juhaokan)
* [@liuxianpan01](https://github.com/liuxianpan01)

## License
Codis is licensed under MIT， see MIT-LICENSE.txt
