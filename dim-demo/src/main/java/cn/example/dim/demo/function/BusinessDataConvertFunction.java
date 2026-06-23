package cn.example.dim.demo.function;

import cn.example.common.demo.constant.Constant;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 进行增删改操作的业务数据
 */
public class BusinessDataConvertFunction extends ProcessFunction<String, JSONObject> {

    @Override
    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSONObject.parse(jsonStr);
        String databaseName = jsonObject.getString(Constant.KAFKA_CDC_DATABASE);
        String type = jsonObject.getString(Constant.KAFKA_CDC_TYPE);
        String data = jsonObject.getString(Constant.KAFKA_CDC_DATA);

        if (Arrays.asList(Constant.KAFKA_CDC_INSERT, Constant.KAFKA_CDC_UPDATE, Constant.KAFKA_CDC_DELETE, Constant.KAFKA_CDC_BOOTSTRAP_INSERT).contains(type.toUpperCase())
                && Constant.MYSQL_DATABASE.equals(databaseName) && StrUtil.isNotBlank(data)) {
            out.collect(jsonObject);
        }
    }

}
