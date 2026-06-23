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
    public static int DWS_ORDER_DETAIL_STATISTICS_SERVER_PORT = 8074;


    // ==================== Redis ====================
    public static final String REDIS_HOST = "127.0.0.1";
    public static final int REDIS_PORT = 6379;
    public static final long DIM_REDIS_EXPIRE = 24 * 60 * 60;


    // ==================== MySQL ====================
    public static final String MYSQL_HOST = "127.0.0.1";
    public static final int MYSQL_PORT = 3316;
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3316/sync_test?useUnicode=true&characterEncoding=utf-8&nullCatalogMeansCurrent=true&useSSL=false&serverTimezone=Asia/Shanghai";
    public static final String MYSQL_USERNAME = "root";
    public static final String MYSQL_PASSWORD = "root";
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
    public static final String KAFKA_BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
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
    /** @Test 本地文件系统 | @Prod hdfs://namenode:9000/checkpoint/xyz2-demo/ */
    public static final String HDFS_NAME_NODE = "file:///E:/flink-checkpoint/xyz2-demo/";


    // ==================== Flink ====================
    /** @Test 并行度3 | @Prod 根据集群资源调整，建议 8~16 */
    public static int PARALLELISM = 3;  //Kafka目前是3分区

    public static long WATERMARK_DELAY = 3;
    public static long WATERMARK_IDLE_TIMEOUT = 10;
    public static long WINDOW_SIZE = 10;
    public static long WINDOW_EXPIRE_TIMEOUT = 10;


    // ==================== StarRocks ====================
    public static final String STARROCKS_JDBC_URL = "jdbc:mysql://127.0.0.1:9030,127.0.0.1:9031,127.0.0.1:9032/";
    public static final String STARROCKS_LOAD_URL = "127.0.0.1:8032";
    public static final String STARROCKS_USERNAME = "root";
    public static final String STARROCKS_PASSWORD = "root";

    /** StarRocks Sink 语义：at-least-once | exactly-once */
    // @Test at-least-once 立即可见 | @Prod exactly-once 保证不丢不重
    public static final String STARROCKS_SINK_SEMANTIC = "exactly-once";
    /** StarRocks Sink 最大重试次数 */
    public static final String STARROCKS_SINK_MAX_RETRIES = "3";
    /** StarRocks Sink 写入模式：append(追加) | upsert(有则更新，无则插入) */
    public static final String STARROCKS_SINK_WRITE_MODE = "upsert";
    /** StarRocks Sink 缓冲区最大行数，达到后触发批量刷新，范围 [64000, 5000000] */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS = "64000";
    /** StarRocks Sink 缓冲区最大字节数，达到后触发批量刷新 */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES = "104857600";  // 100MB
    /** StarRocks Sink 缓冲区刷新间隔（毫秒） @Prod 300000(5分钟) */
    /** @Test 1s 快速可见 | @Prod 300000(5min) */
    public static final String STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS = "1000";

    public static final String DATABASE_DWS = "dws";
    public static final String DWS_CART_STATISTICS = "cart_statistics";
    public static final String DWS_TRADE_SKU_ORDER_STATISTICS = "trade_sku_order_statistics";


    // ==================== HBase ====================
    public static final String HBASE_ZOOKEEPER_QUORUM = "zookeeper1,zookeeper2,zookeeper3:2181";
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

