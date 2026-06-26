package cn.example.common.demo.base;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {

    protected final int port;
    protected final int parallelism;
    protected final String groupId;
    protected final boolean isClusterMode;

    protected BaseSQLApp(int port, int parallelism, String groupId, boolean isClusterMode) {
        this.port = port;
        this.parallelism = parallelism;
        this.groupId = groupId;
        this.isClusterMode = isClusterMode;
    }

    public void run() {
        // 1、创建表执行环境
        StreamTableEnvironment tableEnv;
        if (isClusterMode) {
            tableEnv = cn.example.common.demo.build.FlinkBuilder.createStreamTableEnvironmentForCluster(parallelism, groupId);
        } else {
            tableEnv = cn.example.common.demo.build.FlinkBuilder.createStreamTableEnvironment(port, parallelism, groupId);
        }

        // 2、处理业务数据
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

}
