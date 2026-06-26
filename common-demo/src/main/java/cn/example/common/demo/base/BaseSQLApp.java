package cn.example.common.demo.base;

import cn.example.common.demo.build.FlinkBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /** 匹配 StreamPark 变量占位符：${variable.name} */
    private static final Pattern STREAMPARK_VAR = Pattern.compile("\\$\\{(.+?)}");

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
     * 兼容两种传入方式：
     * 1. Flink 标准：args = ["--cluster", "--mysql.host=mysql8.4.7", ...]
     * 2. StreamPark 单字符串：args = ["--cluster --mysql.host=${mysql.host} ..."]
     *    其中 ${variable} 会自动从 System.getProperty / System.getenv 解析
     */
    protected static void applySystemProperties(String[] args) {
        if (args == null || args.length == 0) return;

        // 1. 如果 StreamPark 把整串参数当作单个数组元素传入，先拆分
        String[] tokens;
        if (args.length == 1 && args[0].contains(" ")) {
            tokens = args[0].split("\\s+");
            System.out.println("[BaseSQLApp] Detected single-string args, split into " + tokens.length + " tokens");
        } else {
            tokens = args;
        }

        // 2. 用 ParameterTool 解析 --key=value 和 --key value
        ParameterTool params = ParameterTool.fromArgs(tokens);

        // 3. 注入 System.setProperty，同时解析 ${variable} 占位符
        for (String key : params.toMap().keySet()) {
            if ("cluster".equals(key)) continue;
            String value = params.get(key);

            // 解析 ${variable.name} 占位符（支持多次出现）
            Matcher m = STREAMPARK_VAR.matcher(value);
            StringBuffer resolved = new StringBuffer();
            while (m.find()) {
                String varName = m.group(1);
                String varValue = System.getProperty(varName);
                if (varValue == null) {
                    varValue = System.getenv(varName);
                }
                if (varValue == null) {
                    varValue = System.getenv(varName.replace('.', '_').toUpperCase());
                }
                m.appendReplacement(resolved, Matcher.quoteReplacement(varValue != null ? varValue : ""));
            }
            m.appendTail(resolved);
            value = resolved.toString();

            System.setProperty(key, value);
            System.out.println("[BaseSQLApp] System.setProperty(" + key + " = " + value + ")");
        }
    }

}
