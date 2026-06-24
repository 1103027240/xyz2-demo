package cn.example.common.demo.function;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.PropertyNamingStrategy;
import com.alibaba.fastjson2.filter.NameFilter;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将Bean转换为JSON字符串，并使用SnakeCase命名策略，将驼峰命名转换为下划线命名（userId转成user_id）
 */
public class BeanToJsonFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        NameFilter nameFilter = NameFilter.of(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, nameFilter);
    }

}
