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
    public static StreamExecutionEnvironment createStreamExecutionEnvironment(int port, int parallelism, String groupId) {
        // 1、执行环境
        StreamExecutionEnvironment env = createBasicStreamExecutionEnvironment(port, parallelism);

        // 2、检查点相关设置
        configCheckpoint(env, groupId);

        return env;
    }

    public static StreamTableEnvironment createStreamTableEnvironment(int port, int parallelism, String groupId) {
        // 1、执行环境
        StreamExecutionEnvironment env = createBasicStreamExecutionEnvironment(port, parallelism);

        // 2、表执行环境（Flink1.20需显式使用EnvironmentSettings，避免CatalogStoreHolder NPE）
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // @Test "0" 表示不过期 | @Prod "7d" 根据业务需要调整
        //tableEnv.getConfig().set("table.exec.state.ttl", "7d");

        // Source空闲时间：超过这个时间，Source变成idle，不阻塞全局水位线
        // @Test 10s | @Prod "60s"
        //tableEnv.getConfig().set("table.exec.source.idle-timeout", "10s");

        // 3、检查点相关设置
        configCheckpoint(env, groupId);

        return tableEnv;
    }

    public static StreamExecutionEnvironment createBasicStreamExecutionEnvironment(int port, int parallelism) {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, port);

        // Java 17+ 兼容HBase：开放java.nio模块反射访问（HBase 2.x Reflection需求）
        conf.setString("env.java.opts",
                "--add-opens java.base/java.nio=ALL-UNNAMED " +
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED " +
                "--add-opens java.base/java.util=ALL-UNNAMED");

        // 使用local环境，确保IDEA可以debug到算子内断点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(parallelism);
        return env;
    }

    public static void configCheckpoint(StreamExecutionEnvironment env, String groupId) {
        // 检查点相关设置
        // 开启检查点：EXACTLY_ONCE，@Prod 30s
        env.enableCheckpointing(30 * 1000, CheckpointingMode.EXACTLY_ONCE); //30s

        // 超时时间：Checkpoint超时直接丢弃
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000); //10min

        // 重试次数：Checkpoint连续重试N次，失败后退出
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // 重启策略：最大重启10次，每次重启时间间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));

        // 最大并发数：同一时间只允许运行的Checkpoint数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 最小间隔：两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5 * 1000); //5s

        /**
         * job取消后检查点是否保留
         * RETAIN_ON_CANCELLATION：取消后保留检查点
         * DELETE_ON_CANCELLATION：取消后删除检查点
         */
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // @Prod 生产环境必须开启：env.setStateBackend(new EmbeddedRocksDBStateBackend(true));  //RocksDB存储，true表示增量Checkpoint
        env.setStateBackend(new HashMapStateBackend()); // 内存存储
        env.getCheckpointConfig().setCheckpointStorage(Constant.HDFS_NAME_NODE + groupId + "/");
    }

}
