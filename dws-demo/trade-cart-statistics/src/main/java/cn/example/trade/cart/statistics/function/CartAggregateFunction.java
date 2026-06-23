package cn.example.trade.cart.statistics.function;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CartAggregateFunction implements AggregateFunction<JSONObject, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(JSONObject jsonObject, Long count) {
        return ++count;
    }

    @Override
    public Long getResult(Long count) {
        return count;
    }

    @Override
    public Long merge(Long count1, Long count2) {
        return count1 + count2;
    }

}
