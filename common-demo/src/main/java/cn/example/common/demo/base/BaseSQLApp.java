package cn.example.common.demo.base;

import cn.example.common.demo.build.FlinkBuilder;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {

    protected final int port;
    protected final int parallelism;
    protected final String groupId;
    protected final boolean isClusterMode;

    /** 本地模式构造器 */
    protected BaseSQLApp(int port, int parallelism, String groupId) {
        this(port, parallelism, groupId, false);
    }

    /** 通用构造器（isClusterMode=true时，使用集群模式） */
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
            tableEnv = FlinkBuilder.createStreamTableEnvironmentForCluster(parallelism, groupId);
        } else {
            tableEnv = FlinkBuilder.createStreamTableEnvironment(port, parallelism, groupId);
        }

        // 2、处理业务数据
        handle(tableEnv);
    }

    public abstract void handle(StreamTableEnvironment tableEnv);

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
