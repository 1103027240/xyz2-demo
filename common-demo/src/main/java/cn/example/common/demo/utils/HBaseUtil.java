package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseUtil {

    //获取HBase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", extractQuorum(Constant.HBASE_ZOOKEEPER_QUORUM));
        config.set("hbase.zookeeper.property.clientPort", extractPort(Constant.HBASE_ZOOKEEPER_QUORUM));
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.rpc.timeout", String.valueOf(60000));
        config.set("hbase.client.retries.number", String.valueOf(3));
        config.set("hbase.client.operation.timeout", String.valueOf(60000));
        return ConnectionFactory.createConnection(config);
    }

    //关闭HBase连接
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    //获取异步HBase连接
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", extractQuorum(Constant.HBASE_ZOOKEEPER_QUORUM));
        config.set("hbase.zookeeper.property.clientPort", extractPort(Constant.HBASE_ZOOKEEPER_QUORUM));
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.rpc.timeout", String.valueOf(60000));
        config.set("hbase.client.retries.number", String.valueOf(3));
        config.set("hbase.client.operation.timeout", String.valueOf(60000));
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(config).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //关闭异步HBase连接
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //建表
    public static void createHBaseTable(Connection hbaseConn, String ns, String table, String... cfs) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tn = TableName.valueOf(ns + ":" + table);
            if (admin.tableExists(tn)) {
                throw new IllegalArgumentException("表已存在: " + tn);
            }

            List<ColumnFamilyDescriptor> families = Arrays.asList(cfs).stream()
                    .map(cf -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build())
                    .collect(Collectors.toList());

            admin.createTable(TableDescriptorBuilder.newBuilder(tn)
                    .setColumnFamilies(families)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException("创建表失败: " + ns + ":" + table, e);
        }
    }

    //删表
    public static void dropHBaseTable(Connection hbaseConn, String ns, String table) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tn = TableName.valueOf(ns + ":" + table);
            if (!admin.tableExists(tn)) {
                throw new IllegalArgumentException("表不存在: " + tn);
            }

            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }

            if (admin.isTableDisabled(tn)) {
                admin.enableTable(tn);
            }
        } catch (Exception e) {
            throw new RuntimeException("删除表失败: " + ns + ":" + table, e);
        }
    }

    public static boolean existsHBaseTable(Connection hbaseConn, String ns, String table) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tn = TableName.valueOf(ns + ":" + table);
            return admin.tableExists(tn);
        } catch (IOException e) {
            throw new RuntimeException("校验表失败: " + ns + ":" + table, e);
        }
    }

    //向表中插入数据
    public static void putRow(Connection hbaseConn, String ns, String table, String rowKey, String family, JSONObject jsonObj) {
        try (Table t = hbaseConn.getTable(TableName.valueOf(ns + ":" + table))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            jsonObj.keySet().forEach(column -> {
                String value = jsonObj.getString(column);
                if (StrUtil.isNotBlank(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            });
            t.put(put);
        } catch (Exception e) {
            throw new RuntimeException("PutRow失败: " + ns + ":" + table + "/" + rowKey, e);
        }
    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String ns, String table, String rowKey) {
        try (Table t = hbaseConn.getTable(TableName.valueOf(ns + ":" + table))) {
            t.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (Exception e) {
            throw new RuntimeException("delRow失败: " + ns + ":" + table + "/" + rowKey, e);
        }
    }

    //查询表中数据
    public static <T> T getRow(Connection hbaseConn, String ns, String table, String rowKey, Class<T> clz, boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  //默认不执行下划线转驼峰
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table t = hbaseConn.getTable(TableName.valueOf(ns + ":" + table))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = t.get(get);
            List<Cell> cells = result.listCells();
            if (CollUtil.isEmpty(cells)) {
                return null;
            }

            T obj = clz.newInstance();
            for (Cell cell : cells) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                if (defaultIsUToC) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            return obj;
        } catch (Exception e) {
            throw new RuntimeException("getRow失败: " + ns + ":" + table + "/" + rowKey, e);
        }
    }

    //以异步方式查询HBase维度表
    public static JSONObject readDimAsync(AsyncConnection asyncConn, String ns, String table, String rowKey) {
        try {
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(TableName.valueOf(ns + ":" + table));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (CollUtil.isEmpty(cells)) {
                return null;
            }

            JSONObject jsonObj = new JSONObject();
            for (Cell cell : cells) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                jsonObj.put(columnName, columnValue);
            }
            return jsonObj;
        } catch (Exception e) {
            throw new RuntimeException("getDimAsync失败: " + ns + ":" + table + "/" + rowKey, e);
        }
    }

    // ==================== ZK Quorum 解析 ====================

    /**
     * 从 "host1,host2,host3:2181" 格式提取 host 列表（去掉末尾端口）
     */
    private static String extractQuorum(String quorumWithPort) {
        int lastColon = quorumWithPort.lastIndexOf(':');
        if (lastColon > 0) {
            return quorumWithPort.substring(0, lastColon);
        }
        return quorumWithPort;
    }

    /**
     * 从 "host1,host2,host3:2181" 格式提取端口
     */
    private static String extractPort(String quorumWithPort) {
        int lastColon = quorumWithPort.lastIndexOf(':');
        if (lastColon > 0 && lastColon < quorumWithPort.length() - 1) {
            return quorumWithPort.substring(lastColon + 1);
        }
        return "2181";
    }

}
