package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * StarRocks Stream Load 连通性诊断工具
 * 1. 先测试 TCP 端口是否可达
 * 2. 再测试 HTTP Stream Load 是否能成功写入
 */
public class StarRocksStreamLoadUtil {

    /**
     * 第一步：纯 TCP 连通性测试（仅检查端口是否可达，不发数据）
     */
    public static void testTcpConnectivity() {
        System.out.println("===== TCP 连通性测试 =====");
        String[] nodes = Constant.STARROCKS_LOAD_URL.split(",");
        for (String node : nodes) {
            String hostPort = node.trim();
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 5000);
                System.out.println("[TCP] " + hostPort + " => 可达 ✓");
            } catch (Exception e) {
                System.out.println("[TCP] " + hostPort + " => 不可达 ✗ (" + e.getMessage() + ")");
            }
        }

        // 也测试 JDBC 端口（MySQL 协议）作对比
        System.out.println("\n----- JDBC 端口对比测试 -----");
        String[] jdbcNodes = Constant.STARROCKS_JDBC_URL.replace("jdbc:mysql://", "").replace("/", "").split(",");
        for (String node : jdbcNodes) {
            String hostPort = node.trim();
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 5000);
                System.out.println("[TCP] " + hostPort + " (JDBC) => 可达 ✓");
            } catch (Exception e) {
                System.out.println("[TCP] " + hostPort + " (JDBC) => 不可达 ✗ (" + e.getMessage() + ")");
            }
        }
    }

    /**
     * 第二步：HTTP Stream Load 写入测试（发一条测试数据）
     */
    public static String testStreamLoad(String databaseName, String tableName, String jsonRow) {
        System.out.println("\n===== HTTP Stream Load 测试 =====");
        // load-url 可能包含多个节点，逐个尝试
        String[] nodes = Constant.STARROCKS_LOAD_URL.split(",");
        for (String node : nodes) {
            String hostPort = node.trim();
            String url = "http://" + hostPort + "/api/" + databaseName + "/" + tableName + "/_stream_load";
            System.out.println("[StreamLoad Test] URL: " + url);

            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestMethod("PUT");
                conn.setDoOutput(true);
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(30000);

                // 认证
                String auth = Constant.STARROCKS_USERNAME + ":" + Constant.STARROCKS_PASSWORD;
                conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8)));

                // Stream Load 参数
                conn.setRequestProperty("format", "json");
                conn.setRequestProperty("strip_outer_array", "true");

                // 发送数据
                conn.getOutputStream().write(jsonRow.getBytes(StandardCharsets.UTF_8));
                conn.getOutputStream().flush();
                conn.getOutputStream().close();

                // 读取响应
                int code = conn.getResponseCode();
                String resp;
                if (code >= 400) {
                    resp = "[HTTP " + code + "] ";
                    byte[] err = conn.getErrorStream().readAllBytes();
                    resp += new String(err, StandardCharsets.UTF_8);
                } else {
                    byte[] body = conn.getInputStream().readAllBytes();
                    resp = "[HTTP " + code + "] " + new String(body, StandardCharsets.UTF_8);
                }
                conn.disconnect();

                System.out.println("[StreamLoad Test] Response: " + resp);
                return resp;

            } catch (Exception e) {
                System.out.println("[StreamLoad Test] " + hostPort + " 连接失败: " + e.getMessage());
            }
        }
        return "所有节点均不可达";
    }

    /**
     * 一键诊断：先测 TCP，再测 HTTP Stream Load
     * 直接在 IDE 中运行此 main 方法即可
     */
    public static void main(String[] args) {
        System.out.println("===== StarRocks Sink 连通性诊断 =====");
        System.out.println("load_url: " + Constant.STARROCKS_LOAD_URL);
        System.out.println("jdbc_url: " + Constant.STARROCKS_JDBC_URL);
        System.out.println();

        // 第一步：TCP 连通性
        testTcpConnectivity();

        // 第二步：如果 TCP 可达，尝试写一条测试数据到 cart_statistics 表
        String testJson = "[{\"dt\":\"2026-06-23\",\"window_start\":\"2026-06-23 19:00:00\",\"window_end\":\"2026-06-23 19:00:10\",\"cart_add_cnt\":1}]";
        testStreamLoad(Constant.DATABASE_DWS, Constant.DWS_CART_STATISTICS, testJson);
    }
}
