package cn.example.trade.cart.add.app;

import cn.example.common.demo.base.BaseSQLApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdCartAddApp extends BaseSQLApp {

    public DwdCartAddApp() {
        super(Constant.DWD_CART_ADD_SERVER_PORT, Constant.PARALLELISM, Constant.DWD_CART_ADD);
    }

    public static void main(String[] args) throws Exception {
        new DwdCartAddApp().run();
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // 1、MysqlCDC读取购物车数据
        tableEnv.executeSql(createDwdCartSourceSQL());

        // 2、过滤加购数据
        Table cartTable = tableEnv.sqlQuery("SELECT\n" +
                "  id,\n" +
                "  user_id,\n" +
                "  sku_id,\n" +
                "  sku_num,\n" +
                "  UNIX_TIMESTAMP(CAST(op_ts AS STRING)) as ts\n" +
                "FROM dwd_cart_info_source\n" +
                "WHERE op_type = '+I'");

        // 3、创建 upsert-kafka 动态表
        tableEnv.executeSql(" create table " + Constant.DWD_CART_ADD + "(\n" +
                "  id BIGINT,\n" +
                "  user_id STRING,\n" +
                "  sku_id BIGINT,\n" +
                "  sku_num INT,\n" +
                "  ts BIGINT,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.DWD_CART_ADD));

        // 4、写入到 upsert-kafka
        cartTable.executeInsert(Constant.DWD_CART_ADD);
    }

    public String createDwdCartSourceSQL() {
        return String.format("CREATE TABLE IF NOT EXISTS dwd_cart_info_source (\n" +
                        "    id BIGINT,\n" +
                        "    user_id STRING,\n" +
                        "    sku_id BIGINT,\n" +
                        "    cart_price DECIMAL(10, 2),\n" +
                        "    sku_num INT,\n" +
                        "    img_url STRING,\n" +
                        "    sku_name STRING,\n" +
                        "    is_checked INT,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
                        "    is_ordered BIGINT,\n" +
                        "    order_time TIMESTAMP(0),\n" +
                        "    op_type STRING METADATA FROM 'row_kind' VIRTUAL,\n" +
                        "    op_ts TIMESTAMP(3) METADATA FROM 'op_ts' VIRTUAL,\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "    'connector' = 'mysql-cdc',\n" +
                        "    'hostname' = '%s',\n" +
                        "    'port' = '%d',\n" +
                        "    'username' = '%s',\n" +
                        "    'password' = '%s',\n" +
                        "    'database-name' = '%s',\n" +
                        "    'table-name' = '%s',\n" +
                        "    'scan.startup.mode' = 'initial',\n" +
                        "    'scan.incremental.snapshot.enabled' = 'true',\n" +
                        "    'debezium.include.schema.changes' = 'false',\n" +
                        "    'debezium.snapshot.locking.mode' = '%s',\n" +
                        "    'server-time-zone' = 'Asia/Shanghai',\n" +
                        "    'connect.timeout' = '%s',\n" +
                        "    'connect.max-retries' = '%s'\n" +
                        ");",
                Constant.MYSQL_HOST,
                Constant.MYSQL_PORT,
                Constant.MYSQL_USERNAME,
                Constant.MYSQL_PASSWORD,
                Constant.MYSQL_DATABASE,
                Constant.TABLE_CRAT_INFO,
                Constant.CDC_SNAPSHOT_LOCKING_MODE,
                Constant.CDC_CONNECT_TIMEOUT,
                Constant.CDC_CONNECT_MAX_RETRIES);
    }

}
