package cn.example.common.demo.function;

import com.alibaba.fastjson2.JSONObject;

public interface DimJoinFunction<T> {

    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;

}
