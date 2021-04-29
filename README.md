# Debezium 结构适配器

提供 `Hudi` 针对 `debezium` 结构变化的捕获和结构变化处理的适配器。

目前主要包括有如下一些 `DDL` 操作的处理：

* 字段的新增
* 字段的重命名

## Docker

```shell
# 打包
docker build -t hudi/debezium-schema-adpapter:1.0 .

# 测试
exoprt HUDI_DBZ_ZK_HOST=your_zookeeper_host
docker run --name hudi-dbz-schema-adpapter \
    -e APP_LOG_LEVEL=TRACE \
    -e HUDI_DBZ_ZK_HOST=$HUDI_DBZ_ZK_HOST \
    -p 8080:8080 -d hudi/debezium-schema-adpapter:1.0
```
