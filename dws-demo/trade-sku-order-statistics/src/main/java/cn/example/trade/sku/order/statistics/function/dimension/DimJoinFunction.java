package cn.example.trade.sku.order.statistics.function.dimension;

import com.alibaba.fastjson2.JSONObject;

public interface DimJoinFunction<T> {

    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;

}
