package cn.example.dim.demo.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TableProcessDim;
import cn.example.common.demo.utils.HBaseUtil;
import cn.example.common.demo.utils.RedisUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    public void invoke(Tuple2<JSONObject, TableProcessDim> tuple, Context ctx) throws Exception {
        System.out.println(String.format(">>> HBaseSinkFunction invoke方法被调用, tuple: %s", tuple));

        JSONObject jsonObj = tuple.f0;
        TableProcessDim tableProcessDim = tuple.f1;

        String type = jsonObj.getString(Constant.KAFKA_CDC_TYPE);
        jsonObj.remove(Constant.KAFKA_CDC_TYPE);

        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        String sinkTable = tableProcessDim.getSinkTable();

        if (Constant.KAFKA_CDC_DELETE.equals(type)) {
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, rowKey, tableProcessDim.getSinkFamily(), jsonObj);
        }

        //如果业务表数据（如维度表）发生了变化（修改或删除），将Redis缓存数据删除
        String key = RedisUtil.getKey(sinkTable, rowKey);
        if (Constant.KAFKA_CDC_INSERT.equals(type.toUpperCase())) {
            RedisUtil.writeDim(jedis, sinkTable, rowKey, jsonObj);
        } else {
            jedis.del(key);
        }
    }

}
