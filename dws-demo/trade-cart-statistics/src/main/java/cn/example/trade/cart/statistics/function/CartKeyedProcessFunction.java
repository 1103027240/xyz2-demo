package cn.example.trade.cart.statistics.function;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CartKeyedProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    private ValueState<String> lastCartDateState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastCartDateState", String.class);
        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
        // 获取上次加购日期
        String lastCartDate = lastCartDateState.value();

        // 获取当前加购日期
        long ts = jsonObject.getLong("ts") * 1000;
        String curCartDate = DateUtil.format(DateUtil.date(ts), "yyyy-MM-dd");

        // 判断是否是加购独立用户
        if (StrUtil.isBlank(lastCartDate) || !lastCartDate.equals(curCartDate)) {
            out.collect(jsonObject);
            lastCartDateState.update(curCartDate);
        }
    }

}
