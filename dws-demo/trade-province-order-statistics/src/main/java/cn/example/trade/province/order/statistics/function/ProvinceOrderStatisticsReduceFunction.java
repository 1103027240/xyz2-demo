package cn.example.trade.province.order.statistics.function;

import cn.example.common.demo.entity.TradeProvinceOrderStatistics;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ProvinceOrderStatisticsReduceFunction implements ReduceFunction<TradeProvinceOrderStatistics> {

    @Override
    public TradeProvinceOrderStatistics reduce(TradeProvinceOrderStatistics value1, TradeProvinceOrderStatistics value2) throws Exception {
        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
        return value1;
    }

}
