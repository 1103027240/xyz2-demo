package cn.example.common.demo.base;

import cn.example.common.demo.build.FlinkBuilder;
import cn.example.common.demo.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseApp {

    protected final int port;
    protected final int parallelism;
    protected final String topic;
    protected final String groupId;
    protected final boolean isClusterMode;

    /** 本地模式构造器 */
    protected BaseApp(int port, int parallelism, String topic, String groupId) {
        this(port, parallelism, topic, groupId, false);
    }

    /** 通用构造器（isClusterMode=true 时使用集群模式） */
    protected BaseApp(int port, int parallelism, String topic, String groupId, boolean isClusterMode) {
        this.port = port;
        this.parallelism = parallelism;
        this.topic = topic;
        this.groupId = groupId;
        this.isClusterMode = isClusterMode;
    }

    public void run() throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env;
        if (isClusterMode) {
            env = FlinkBuilder.createStreamExecutionEnvironmentForCluster(parallelism, groupId);
        } else {
            env = FlinkBuilder.createStreamExecutionEnvironment(port, parallelism, groupId);
        }

        // 2、从Kafka读取业务数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        kafkaDS.print("kafkaDS");

        // 3、处理业务数据
        handle(env, kafkaDS);

        // 4、提交作业
        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS);

    /** 检查启动参数是否包含 --cluster 标志 */
    protected static boolean hasClusterFlag(String[] args) {
        if (args != null) {
            for (String arg : args) {
                if ("--cluster".equals(arg)) {
                    return true;
                }
            }
        }
        return false;
    }

}
