package cn.example.trade.sku.order.statistics.function;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import org.apache.flink.api.common.functions.ReduceFunction;

public class SkuOrderStatisticsReduceFunction implements ReduceFunction<TradeSkuOrderStatistics> {

    @Override
    public TradeSkuOrderStatistics reduce(TradeSkuOrderStatistics value1, TradeSkuOrderStatistics value2) throws Exception {
        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
        return value1;
    }

}
