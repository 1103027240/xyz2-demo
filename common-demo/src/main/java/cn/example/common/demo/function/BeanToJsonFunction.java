package cn.example.common.demo.function;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.functions.MapFunction;

public class BeanToJsonFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        return JSON.toJSONString(bean);
    }

}
