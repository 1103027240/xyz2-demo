package cn.example.trade.sku.order.statistics.function.dimension;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.utils.HBaseUtil;
import cn.example.common.demo.utils.RedisUtil;
import com.alibaba.fastjson2.JSONObject;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private AsyncConnection hbaseAsyncConn;
    private StatefulRedisConnection<String, String> redisAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(
                () -> {
                    String key = getRowKey(obj);
                    JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), key);
                    return dimJsonObj;
                }
        ).thenApplyAsync(
                dimJsonObj -> {
                    if (dimJsonObj != null) {
                        System.out.println(String.format("~~~从Redis中，找到了表[%s]的RowKey[%s]数据~~~", getTableName(), getRowKey(obj)));
                    } else {
                        dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(obj));
                        if (dimJsonObj != null) {
                            RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(obj), dimJsonObj);
                        } else {
                            System.out.println(String.format("~~~没有找到表[%s]的RowKey[%s]数据~~~", getTableName(), getRowKey(obj)));
                        }
                    }
                    return dimJsonObj;
                }
        ).thenAcceptAsync(
                dimJsonObj -> {
                    if (dimJsonObj != null) {
                        addDims(obj, dimJsonObj);
                    }
                    resultFuture.complete(Collections.singleton(obj));
                }
        );
    }

}
