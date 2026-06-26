package cn.example.common.demo.build;

import cn.example.common.demo.constant.Constant;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkBuilder {

    // ==================== 本地模式 ====================

    public static StreamExecutionEnvironment createStreamExecutionEnvironment(int port, int parallelism, String groupId) {
        StreamExecutionEnvironment env = createBasicStreamExecutionEnvironment(port, parallelism);
        configCheckpoint(env, groupId);
        return env;
    }

    public static StreamTableEnvironment createStreamTableEnvironment(int port, int parallelism, String groupId) {
        StreamExecutionEnvironment env = createBasicStreamExecutionEnvironment(port, parallelism);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        configCheckpoint(env, groupId);
        return tableEnv;
    }

    public static StreamExecutionEnvironment createBasicStreamExecutionEnvironment(int port, int parallelism) {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, port);

        conf.setString("env.java.opts",
                "--add-opens java.base/java.nio=ALL-UNNAMED " +
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED " +
                "--add-opens java.base/java.util=ALL-UNNAMED");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(parallelism);
        return env;
    }

    // ==================== 集群模式 ====================

    public static StreamExecutionEnvironment createStreamExecutionEnvironmentForCluster(int parallelism, String groupId) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        configCheckpoint(env, groupId);
        return env;
    }

    public static StreamTableEnvironment createStreamTableEnvironmentForCluster(int parallelism, String groupId) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        configCheckpoint(env, groupId);
        return tableEnv;
    }

    // ==================== Checkpoint 配置 ====================

    public static void configCheckpoint(StreamExecutionEnvironment env, String groupId) {
        env.enableCheckpointing(30 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(Constant.HDFS_NAME_NODE + groupId + "/");
    }

}
