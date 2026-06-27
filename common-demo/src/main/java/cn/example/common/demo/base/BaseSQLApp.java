package cn.example.common.demo.base;

import cn.example.common.demo.build.FlinkBuilder;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {

    protected final int port;
    protected final int parallelism;
    protected final String groupId;

    protected BaseSQLApp(int port, int parallelism, String groupId) {
        this.port = port;
        this.parallelism = parallelism;
        this.groupId = groupId;
    }

    public void run() {
        // 1、创建表执行环境（本地模式 + WebUI）
        // 生产：StreamExecutionEnvironment.getExecutionEnvironment()
        StreamTableEnvironment tableEnv = FlinkBuilder.createStreamTableEnvironment(port, parallelism, groupId);

        // 2、处理业务数据
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

}
