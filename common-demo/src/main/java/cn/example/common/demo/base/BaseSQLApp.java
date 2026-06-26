package cn.example.common.demo.base;

import cn.example.common.demo.build.FlinkBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
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

    /**
     * 将 --key=value 格式的程序参数注入 System.setProperty
     * 需在 Constant 类加载前调用，确保 System.getProperty() 能拿到值
     */
    protected static void applySystemProperties(String[] args) {
        if (args == null || args.length == 0) return;
        ParameterTool params = ParameterTool.fromArgs(args);
        for (String key : params.toMap().keySet()) {
            if ("cluster".equals(key)) continue;
            System.setProperty(key, params.get(key));
        }
    }

}
