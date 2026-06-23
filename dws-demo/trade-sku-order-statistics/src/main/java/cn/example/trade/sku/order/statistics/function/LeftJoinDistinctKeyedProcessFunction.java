package cn.example.trade.sku.order.statistics.function;

import cn.example.common.demo.constant.Constant;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 解决左外连接数据先来后到问题
 * 1）状态 + 定时器（延迟）
 * 2）状态 + 撤销（数据量大）：当前数据1条 + 撤销数据1条
 */
public class LeftJoinDistinctKeyedProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private ValueState<JSONObject> lastJsonObjState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(Constant.WINDOW_EXPIRE_TIMEOUT)).build());
        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject lastJsonObj = lastJsonObjState.value();
        if (lastJsonObj != null) {
            String splitOriginalAmount = lastJsonObj.getString(Constant.SPLIT_ORIGINAL_AMOUNT);
            String splitActivityAmount = lastJsonObj.getString(Constant.SPLIT_ACTIVITY_AMOUNT);
            String splitCouponAmount = lastJsonObj.getString(Constant.SPLIT_COUPON_AMOUNT);
            String splitTotalAmount = lastJsonObj.getString(Constant.SPLIT_TOTAL_AMOUNT);
            lastJsonObj.put(Constant.SPLIT_ORIGINAL_AMOUNT, String.format("-%s", splitOriginalAmount));
            lastJsonObj.put(Constant.SPLIT_ACTIVITY_AMOUNT, String.format("-%s", splitActivityAmount));
            lastJsonObj.put(Constant.SPLIT_COUPON_AMOUNT, String.format("-%s", splitCouponAmount));
            lastJsonObj.put(Constant.SPLIT_TOTAL_AMOUNT, String.format("-%s", splitTotalAmount));
            out.collect(lastJsonObj);
        }
        out.collect(jsonObject);
    }

}
