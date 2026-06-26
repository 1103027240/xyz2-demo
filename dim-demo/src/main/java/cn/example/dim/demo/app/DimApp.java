package cn.example.dim.demo.app;

import cn.example.common.demo.base.BaseApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TableProcessDim;
import cn.example.common.demo.utils.FlinkSourceUtil;
import cn.example.dim.demo.function.*;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 业务数据表或配置表数据变更同步到HBase
 */
public class DimApp extends BaseApp {

    public DimApp() {
        super(Constant.DIM_TABLE_SERVER_PORT, Constant.PARALLELISM, Constant.KAFKA_TOPIC_DB, Constant.DIM_APP);
    }

    public DimApp(boolean isClusterMode) {
        super(Constant.DIM_TABLE_SERVER_PORT, Constant.PARALLELISM, Constant.KAFKA_TOPIC_DB, Constant.DIM_APP, isClusterMode);
    }

    /**
     * 启动参数（key=value 格式自动注入 System.setProperty）
     * --cluster                          使用集群模式
     * --mysql.host=${mysql.host}         MySQL主机
     * --mysql.port=${mysql.port}         MySQL端口
     * --redis.host=${redis.host}         Redis主机
     * --kafka.brokers=${kafka.brokers}   Kafka地址
     * --starrocks.jdbc.url=...           StarRocks JDBC
     * --starrocks.load.url=...           StarRocks Load
     * --hbase.zookeeper.quorum=...       HBase ZK
     * --hdfs.namenode=...                HDFS地址
     * --mysql.username=... --mysql.password=... 等
     */
    public static void main(String[] args) throws Exception {
        applySystemProperties(args);
        boolean isClusterMode = hasClusterFlag(args);
        new DimApp(isClusterMode).run();
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        // 1、将业务数据转成JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new BusinessDataConvertFunction());

        // 2、读取Mysql配置表数据操作
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimDS = readFromTableProcessDim(env);

        // 3、将配置表数据操作，转成HBase表操作
        tableProcessDimDS.map(new TableProcessDimSinkFunction()).setParallelism(1);

        // 4、将业务数据同步到HBase
        syncToHBase(jsonObjDS, tableProcessDimDS);
    }

    // 进行增删改查操作的配置表数据
    private SingleOutputStreamOperator<TableProcessDim> readFromTableProcessDim(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.MYSQL_DATABASE, Constant.TABLE_PROCESS_DIM);
        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql_Source")
                .setParallelism(1)
                .map(new TableProcessDimSourceFunction());
    }

    private void syncToHBase(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tableProcessDimDS) {
        // 将配置表转成广播流
        MapStateDescriptor<String, TableProcessDim> tableProcessDimMapStateDescriptor = new MapStateDescriptor<>("tableProcessDimMapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tableProcessDimDS.broadcast(tableProcessDimMapStateDescriptor);

        // 将业务流与广播流关联
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectDS = jsonObjDS.connect(broadcastDS)
                .process(new TableProcessDimBroadcastFunction(tableProcessDimMapStateDescriptor));

        // 将业务数据同步到HBase
        connectDS.addSink(new HBaseSinkFunction());
    }

}
