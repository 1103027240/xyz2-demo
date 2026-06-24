package cn.example.trade.order.detail.app;

import cn.example.common.demo.base.BaseSQLApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;

public class DwdOrderDetailApp extends BaseSQLApp {

    public DwdOrderDetailApp() {
        super(Constant.DWD_ORDER_DETAIL_SERVER_PORT, Constant.PARALLELISM, Constant.DWD_ORDER_DETAIL);
    }

    /**
     * 启动参数
     * --add-opens java.base/java.nio=ALL-UNNAMED
     * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
     * --add-opens java.base/java.lang=ALL-UNNAMED
     * --add-opens java.base/java.util=ALL-UNNAMED
     */
    public static void main(String[] args) throws Exception {
        new DwdOrderDetailApp().run();
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // 设置状态保留时间（传输延迟 + 业务上滞后关系）
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 1、过滤订单明细数据
        tableEnv.executeSql(createDwdOrderDetailSourceSQL());

        // 2、过滤订单数据
        tableEnv.executeSql(createDwdOrderInfoSourceSQL());

        // 3、过滤订单明细活动数据
        tableEnv.executeSql(createDwdOrderDetailActivitySourceSQL());

        // 4、过滤订单明细优惠券数据
        tableEnv.executeSql(createDwdOrderDetailCouponSourceSQL());

        // 5、关联各表数据
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "  cast(od.id as String) AS id,\n" +
                "  cast(od.order_id as String) AS order_id,\n" +
                "  cast(oi.user_id as String) AS user_id,\n" +
                "  cast(od.sku_id as String) sku_id,\n" +
                "  od.sku_name,\n" +
                "  cast(oi.province_id as String) AS province_id,\n" +
                "  cast(act.activity_id as String) AS activity_id,\n" +
                "  cast(act.activity_rule_id as String) AS activity_rule_id,\n" +
                "  cast(cou.coupon_id as String) AS coupon_id,\n" +
                "  date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "  cast(od.create_time as String) AS create_time,\n" +
                "  cast(od.sku_num as String) AS sku_num,\n" +
                "  cast((od.sku_num * od.order_price) as String) AS split_original_amount," +
                "  cast(od.split_activity_amount as String) AS split_activity_amount,\n" +
                "  cast(od.split_coupon_amount as String) AS split_coupon_amount,\n" +
                "  cast(od.split_total_amount as String) AS split_total_amount,\n" +
                "  UNIX_TIMESTAMP(CAST(od.op_ts AS STRING)) as ts\n" +
                "from dwd_order_detail_source od\n" +
                "inner join dwd_order_info_source oi on od.order_id = oi.id\n" +
                "left join dwd_order_detail_activity_source act on od.id = act.order_detail_id\n" +
                "left join dwd_order_detail_coupon_source cou on od.id = cou.order_detail_id\n"
        );

        // 6、创建upsert-kafka动态表
        tableEnv.executeSql("create table " + Constant.DWD_ORDER_DETAIL + "(\n" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint," +
                "primary key(id) not enforced " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.DWD_ORDER_DETAIL));

        // 7、写入到upsert-kafka
        resultTable.executeInsert(Constant.DWD_ORDER_DETAIL);
    }

    public String createDwdOrderDetailSourceSQL() {
        return String.format("CREATE TABLE IF NOT EXISTS dwd_order_detail_source (\n" +
                        "    id BIGINT,\n" +
                        "    order_id BIGINT,\n" +
                        "    sku_id BIGINT,\n" +
                        "    sku_name STRING,\n" +
                        "    img_url STRING,\n" +
                        "    order_price DECIMAL(10, 2),\n" +
                        "    sku_num BIGINT,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    split_total_amount DECIMAL(16, 2),\n" +
                        "    split_activity_amount DECIMAL(16, 2),\n" +
                        "    split_coupon_amount DECIMAL(16, 2),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
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
                        "    'scan.startup.mode' = '%s',\n" +
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
                Constant.TABLE_ORDER_DETAIL,
                Constant.CDC_SCAN_STARTUP_MODE,
                Constant.CDC_SNAPSHOT_LOCKING_MODE,
                Constant.CDC_CONNECT_TIMEOUT,
                Constant.CDC_CONNECT_MAX_RETRIES);
    }

    public String createDwdOrderInfoSourceSQL() {
        return String.format("CREATE TABLE IF NOT EXISTS dwd_order_info_source (\n" +
                        "    id BIGINT,\n" +
                        "    consignee STRING,\n" +
                        "    consignee_tel STRING,\n" +
                        "    total_amount DECIMAL(10, 2),\n" +
                        "    order_status STRING,\n" +
                        "    user_id BIGINT,\n" +
                        "    payment_way STRING,\n" +
                        "    delivery_address STRING,\n" +
                        "    order_comment STRING,\n" +
                        "    out_trade_no STRING,\n" +
                        "    trade_body STRING,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
                        "    expire_time TIMESTAMP(0),\n" +
                        "    process_status STRING,\n" +
                        "    tracking_no STRING,\n" +
                        "    parent_order_id BIGINT,\n" +
                        "    img_url STRING,\n" +
                        "    province_id INT,\n" +
                        "    activity_reduce_amount DECIMAL(16, 2),\n" +
                        "    coupon_reduce_amount DECIMAL(16, 2),\n" +
                        "    original_total_amount DECIMAL(16, 2),\n" +
                        "    feight_fee DECIMAL(16, 2),\n" +
                        "    feight_fee_reduce DECIMAL(16, 2),\n" +
                        "    refundable_time TIMESTAMP(0),\n" +
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
                        "    'scan.startup.mode' = '%s',\n" +
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
                Constant.TABLE_ORDER_INFO,
                Constant.CDC_SCAN_STARTUP_MODE,
                Constant.CDC_SNAPSHOT_LOCKING_MODE,
                Constant.CDC_CONNECT_TIMEOUT,
                Constant.CDC_CONNECT_MAX_RETRIES);
    }

    public String createDwdOrderDetailActivitySourceSQL() {
        return String.format("CREATE TABLE IF NOT EXISTS dwd_order_detail_activity_source (\n" +
                        "    id BIGINT,\n" +
                        "    order_id BIGINT,\n" +
                        "    order_detail_id BIGINT,\n" +
                        "    activity_id BIGINT,\n" +
                        "    activity_rule_id BIGINT,\n" +
                        "    sku_id BIGINT,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
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
                        "    'scan.startup.mode' = '%s',\n" +
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
                Constant.TABLE_ORDER_DETAIL_ACTIVITY,
                Constant.CDC_SCAN_STARTUP_MODE,
                Constant.CDC_SNAPSHOT_LOCKING_MODE,
                Constant.CDC_CONNECT_TIMEOUT,
                Constant.CDC_CONNECT_MAX_RETRIES);
    }

    public String createDwdOrderDetailCouponSourceSQL(){
        return String.format("CREATE TABLE IF NOT EXISTS dwd_order_detail_coupon_source (\n" +
                        "    id BIGINT,\n" +
                        "    order_id BIGINT,\n" +
                        "    order_detail_id BIGINT,\n" +
                        "    coupon_id BIGINT,\n" +
                        "    coupon_use_id BIGINT,\n" +
                        "    sku_id BIGINT,\n" +
                        "    create_time TIMESTAMP(0),\n" +
                        "    operate_time TIMESTAMP(0),\n" +
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
                        "    'scan.startup.mode' = '%s',\n" +
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
                Constant.TABLE_ORDER_DETAIL_COUPON,
                Constant.CDC_SCAN_STARTUP_MODE,
                Constant.CDC_SNAPSHOT_LOCKING_MODE,
                Constant.CDC_CONNECT_TIMEOUT,
                Constant.CDC_CONNECT_MAX_RETRIES);
    }

}
