package cn.example.dim.demo.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TableProcessDim;
import cn.example.common.demo.utils.HBaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Mysql配置表操作
 * 插入数据：HBase插入对应表
 * 修改数据：HBase先删后插入对应表
 * 删除数据：HBase删除对应表
 * 查询数据：HBase不存在该表，就插入对应表；存在就不作处理
 */
public class TableProcessDimSinkFunction extends RichMapFunction<TableProcessDim, TableProcessDim> {

    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    @Override
    public TableProcessDim map(TableProcessDim tp) throws Exception {
        String op = tp.getOp();
        String sinkTable = tp.getSinkTable();
        String[] sinkFamilyArr = tp.getSinkFamily().split(",");

        if (Constant.MYSQL_CDC_CREATE.equals(op)) {
            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilyArr);
        } else if (Constant.MYSQL_CDC_UPDATE.equals(op)) {
            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilyArr);
        } else if (Constant.MYSQL_CDC_DELETE.equals(op)) {
            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
        } else {
            if (!HBaseUtil.existsHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable)) {
                HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilyArr);
            }
        }

        return tp;
    }

}
