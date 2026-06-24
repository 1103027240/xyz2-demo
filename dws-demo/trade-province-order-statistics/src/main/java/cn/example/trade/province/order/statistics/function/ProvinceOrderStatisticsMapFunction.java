package cn.example.trade.province.order.statistics.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TradeProvinceOrderStatistics;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

public class ProvinceOrderStatisticsMapFunction implements MapFunction<JSONObject, TradeProvinceOrderStatistics> {

    @Override
    public TradeProvinceOrderStatistics map(JSONObject jsonObj) throws Exception {
        String provinceId = jsonObj.getString("province_id");
        BigDecimal splitTotalAmount = jsonObj.getBigDecimal(Constant.SPLIT_TOTAL_AMOUNT);
        Long ts = jsonObj.getLong("ts") * 1000;
        String orderId = jsonObj.getString("order_id");

        return TradeProvinceOrderStatistics.builder()
                .provinceId(provinceId)
                .orderAmount(splitTotalAmount)
                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                .ts(ts)
                .build();
    }

}
