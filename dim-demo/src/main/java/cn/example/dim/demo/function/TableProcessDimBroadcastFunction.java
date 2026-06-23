package cn.example.dim.demo.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TableProcessDim;
import cn.example.common.demo.utils.JdbcUtil;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

public class TableProcessDimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> tableDimMapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>(); //解决两条流数据先来后到问题

    public TableProcessDimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> tableDimMapStateDescriptor) {
        this.tableDimMapStateDescriptor = tableDimMapStateDescriptor;
    }

    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();

        // 数据先加载到内存（宕机恢复时也要从Checkpoint加载到内存，目前没实现）
        configMap = JdbcUtil.queryList(mySQLConnection, "SELECT * FROM " + Constant.MYSQL_DATABASE + "." + Constant.TABLE_PROCESS_DIM, TableProcessDim.class, true)
                .stream()
                .collect(Collectors.toMap(TableProcessDim::getSinkTable, e -> e, (e1, e2) -> e2));

        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    /**
     * 将业务数据与配置表数据关联
     */
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        System.out.println(String.format(">>> TableProcessDimBroadcastFunction processElement被调用, jsonObject: %s", jsonObject));

        // 获取配置表数据
        String table = jsonObject.getString(Constant.KAFKA_CDC_TABLE);
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(tableDimMapStateDescriptor);
        TableProcessDim tableProcessDim = Optional.ofNullable(broadcastState.get(table)).orElse(Optional.ofNullable(configMap.get(table)).orElse(null));

        // 将业务数据与配置表数据关联
        if (tableProcessDim != null) {
            JSONArray jsonArray = jsonObject.getJSONArray(Constant.KAFKA_CDC_DATA);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObj = jsonArray.getJSONObject(i);

                // 配置表过滤业务数据不需要的字段
                String sinkColumns = tableProcessDim.getSinkColumns();
                deleteNotNeedColumns(jsonObj, sinkColumns);

                // 业务数据添加操作类型
                String type = jsonObject.getString(Constant.KAFKA_CDC_TYPE);
                jsonObj.put(Constant.KAFKA_CDC_TYPE, type);

                out.collect(Tuple2.of(jsonObj, tableProcessDim));
            }
        }
    }

    /**
     * 将配置表数据存储在状态中
     */
    @Override
    public void processBroadcastElement(TableProcessDim tp, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();
        String sourceTable = tp.getSourceTable();
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(tableDimMapStateDescriptor);

        if (Constant.MYSQL_CDC_DELETE.equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
        }
    }

    private void deleteNotNeedColumns(JSONObject jsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        jsonObj.entrySet().removeIf(e -> !columnList.contains(e.getKey()));
    }

}
