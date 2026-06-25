package cn.example.common.demo.constant;

/**
 * 全局配置常量
 * @Test — 当前值,本地
 * @Prod — 生产推荐值
 */
public class Constant {

    // ==================== 作业端口 ====================
    /**
     * http://localhost:8070/jobs
     * http://localhost:8070/jobs/{id}：id取上面查询结果id
     */
    public static int DIM_TABLE_SERVER_PORT = 8070;
    public static int DWD_CART_ADD_SERVER_PORT = 8071;
    public static int DWD_ORDER_DETAIL_SERVER_PORT = 8072;
    public static int DWS_CART_STATISTICS_SERVER_PORT = 8073;
    public static int DWS_SKU_ORDER_STATISTICS_SERVER_PORT = 8074;
    public static int DWS_PROVINCE_ORDER_STATISTICS_SERVER_PORT = 8075;


    // ==================== Redis ====================
    /** @Local：127.0.0.1 | @StreamPark：redis-8.2.4 **/
    public static final String REDIS_HOST = System.getProperty("redis.host", "127.0.0.1");
    public static final int REDIS_PORT = 6379;
    public static final long DIM_REDIS_EXPIRE = 24 * 60 * 60;


    // ==================== MySQL ====================
    /** @Local：127.0.0.1 | @StreamPark：mysql8.4.7 **/
    public static final String MYSQL_HOST = System.getProperty("mysql.host", "127.0.0.1");
    /** @Local：3316 | @StreamPark：3306 **/
    public static final int MYSQL_PORT = Integer.parseInt(System.getProperty("mysql.port", "3316"));
    /** 由 MYSQL_HOST + MYSQL_PORT 拼接，无需单独配置 **/
    public static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/sync_test?useUnicode=true&characterEncoding=utf-8&nullCatalogMeansCurrent=true&useSSL=false&serverTimezone=Asia/Shanghai";
    /** @Local：root | @StreamPark：root **/
    public static final String MYSQL_USERNAME = System.getProperty("mysql.username", "root");
    /** @Local：root | @StreamPark：root **/
    public static final String MYSQL_PASSWORD = System.getProperty("mysql.password", "root");
    public static final String MYSQL_DATABASE = "sync_test";
    public static final String TABLE_PROCESS_DIM = "table_process_dim";
    public static final String TABLE_CRAT_INFO = "cart_info";
    public static final String TABLE_ORDER_DETAIL = "order_detail";
    public static final String TABLE_ORDER_INFO = "order_info";
    public static final String TABLE_ORDER_DETAIL_ACTIVITY = "order_detail_activity";
    public static final String TABLE_ORDER_DETAIL_COUPON = "order_detail_coupon";


    // ==================== MySQL CDC ====================
    public static final String MYSQL_CDC_BEFORE = "before";
    public static final String MYSQL_CDC_AFTER = "after";

    public static final String MYSQL_CDC_OP = "op";
    public static final String MYSQL_CDC_CREATE = "c";
    public static final String MYSQL_CDC_UPDATE = "u";
    public static final String MYSQL_CDC_DELETE = "d";
    public static final String MYSQL_CDC_SELECT = "r";

    /**
     * Debezium 快照锁模式
     * minimal: 仅在获取表 schema 时短暂加锁，随后释放（生产环境建议 minimal）
     * none:   完全不锁表（适用于容忍快照不一致的场景，如从备库读取）
     */
    public static String CDC_SNAPSHOT_LOCKING_MODE = "minimal";

    /** MySQL CDC 连接超时（毫秒） */
    public static String CDC_CONNECT_TIMEOUT = "30000";

    /** MySQL CDC 最大重试次数 */
    public static String CDC_CONNECT_MAX_RETRIES = "3";

    /** MySQL CDC 启动模式：initial(全量+增量) | latest-offset(仅增量) */
    public static String CDC_SCAN_STARTUP_MODE = "initial";


    // ==================== Kafka ====================
    /** @Local：kafka-1:9092,kafka-2:9092,kafka-3:9092 | @StreamPark：kafka-1:9092,kafka-2:9092,kafka-3:9092 **/
    public static final String KAFKA_BROKERS = System.getProperty("kafka.brokers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
    public static final String KAFKA_TOPIC_DB = "canal-topic";


    // ==================== Kafka CDC ====================
    public static final String KAFKA_CDC_DATABASE = "database";
    public static final String KAFKA_CDC_TABLE = "table";
    public static final String KAFKA_CDC_DATA = "data";

    public static final String KAFKA_CDC_TYPE = "type";
    public static final String KAFKA_CDC_INSERT = "INSERT";
    public static final String KAFKA_CDC_UPDATE = "UPDATE";
    public static final String KAFKA_CDC_DELETE = "DELETE";
    public static final String KAFKA_CDC_BOOTSTRAP_INSERT = "BOOTSTRAP-INSERT";   //全量同步


    // ==================== HDFS ====================
    /** @Local：file:///E:/flink-checkpoint/xyz2-demo/（默认） | @StreamPark：hdfs://namenode:9000/checkpoint/xyz2-demo/ */
    public static final String HDFS_NAME_NODE = System.getProperty("hdfs.namenode", "file:///E:/flink-checkpoint/xyz2-demo/");


    // ==================== Flink ====================
    /** @Local：3（Kafka当前3分区） | @StreamPark：根据集群资源调整，建议 8~16 */
    public static int PARALLELISM = Integer.getInteger("flink.parallelism", 3);

    public static long WATERMARK_DELAY = 3;
    public static long WATERMARK_IDLE_TIMEOUT = 5;
    public static long WINDOW_SIZE = 10;
    public static long WINDOW_EXPIRE_TIMEOUT = 30;
    public static long WATERMARK_OUT_OF_ORDER = 3;


    // ==================== StarRocks ====================
    /** @Local：jdbc:mysql://127.0.0.1:9030,127.0.0.1:9031,127.0.0.1:9032/（默认） | @StreamPark：jdbc:mysql://starrocks-fe-0:9030,starrocks-fe-1:9030,starrocks-fe-2:9030/ **/
    public static final String STARROCKS_JDBC_URL = System.getProperty("starrocks.jdbc.url", "jdbc:mysql://127.0.0.1:9030,127.0.0.1:9031,127.0.0.1:9032/");
    /** @Local：127.0.0.1:8032 | @StreamPark：starrocks-fe-0:8030,starrocks-fe-1:8030,starrocks-fe-2:8030 **/
    public static final String STARROCKS_LOAD_URL = System.getProperty("starrocks.load.url", "127.0.0.1:8032");
    public static final String STARROCKS_USERNAME = "root";
    public static final String STARROCKS_PASSWORD = "root";

    /** @Test at-least-once | @Prod exactly-once 保证不丢不重 */
    public static final String STARROCKS_SINK_SEMANTIC = "exactly-once";
    /** 最大重试次数 */
    public static final String STARROCKS_SINK_MAX_RETRIES = "3";
    /** 写入模式：append(追加) | upsert(有则更新，无则插入) */
    public static final String STARROCKS_SINK_WRITE_MODE = "upsert";
    /** 缓冲区最大行数，达到后触发批量刷新，范围 [64000, 5000000] */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS = "64000";
    /** 缓冲区最大字节数，达到后触发批量刷新 */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES = "104857600";  // 100MB
    /** @Test 1s | @Prod 300000(5min) */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS = "1000";

    public static final String DATABASE_DWS = "dws";
    public static final String DWS_CART_STATISTICS = "cart_statistics";
    public static final String DWS_TRADE_SKU_ORDER_STATISTICS = "trade_sku_order_statistics";
    public static final String DWS_TRADE_PROVINCE_ORDER_STATISTICS = "trade_province_order_statistics";


    // ==================== HBase ====================
    /** @Local：zookeeper1,zookeeper2,zookeeper3:2181 | @StreamPark：zookeeper1,zookeeper2,zookeeper3:2181 **/
    public static final String HBASE_ZOOKEEPER_QUORUM = System.getProperty("hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3:2181");
    public static final String HBASE_NAMESPACE = "XYZ2_DEMO";


    // ==================== 基础配置 ====================
    public static final String DIM_APP = "dim_app";
    public static final String DWD_CART_ADD = "dwd_cart_add";
    public static final String DWD_ORDER_DETAIL = "dwd_order_detail";

    public static final String SPLIT_ORIGINAL_AMOUNT = "split_original_amount";
    public static final String SPLIT_ACTIVITY_AMOUNT = "split_activity_amount";
    public static final String SPLIT_COUPON_AMOUNT = "split_coupon_amount";
    public static final String SPLIT_TOTAL_AMOUNT = "split_total_amount";

}

