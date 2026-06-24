package cn.example.trade.province.order.statistics.function;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ParameterFilterMapFunction extends ProcessFunction<String, JSONObject> {

    @Override
    public void processElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        if (StrUtil.isNotBlank(jsonStr)) {
            out.collect(JSON.parseObject(jsonStr));
        }
    }

}
