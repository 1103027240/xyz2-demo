package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;

public class SQLUtil {

    /**
     * 获取kafka连接器的连接属性
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取upsert-kafka连接器的连接属性
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    /**
     * 获取Hbase连接器的连接属性
     */
    public static String getHBaseDDL(String namespace, String tableName) {
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_QUORUM + "',\n" +
                " 'table-name' = '" + namespace + ":" + tableName + "',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '5000',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

}
