package cn.example.common.demo.base;

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
        // 1、创建表执行环境（延迟引用，避免 Constant 提前加载）
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

    /** 检查启动参数是否包含 --cluster 标志（兼容单字符串 args） */
    protected static boolean hasClusterFlag(String[] args) {
        if (args == null || args.length == 0) return false;

        if (args.length == 1 && args[0].contains(" ")) {
            return args[0].contains("--cluster");
        }

        for (String arg : args) {
            if ("--cluster".equals(arg)) return true;
        }
        return false;
    }

    /**
     * 将 --key=value 格式的程序参数注入 System.setProperty
     * 需在 Constant 类加载前调用，确保 System.getProperty() 能拿到值
     *
     * 兼容 StreamPark 单字符串传入方式：
     * args = ["--cluster --mysql.host=mysql8.4.7 ..."] 会先拆分为标准格式再解析
     */
    protected static void applySystemProperties(String[] args) {
        if (args == null || args.length == 0) return;

        String[] tokens;
        if (args.length == 1 && args[0].contains(" ")) {
            tokens = args[0].split("\\s+");
        } else {
            tokens = args;
        }

        ParameterTool params = ParameterTool.fromArgs(tokens);
        for (String key : params.toMap().keySet()) {
            if ("cluster".equals(key)) continue;
            System.setProperty(key, params.get(key));
        }
    }

}
