package cn.example.trade.sku.order.statistics.function;

import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import java.math.BigDecimal;

public class SkuOrderStatisticsMapFunction implements MapFunction<JSONObject, TradeSkuOrderStatistics> {

    @Override
    public TradeSkuOrderStatistics map(JSONObject jsonObj) throws Exception {
        String skuId = jsonObj.getString("sku_id");
        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal(Constant.SPLIT_ORIGINAL_AMOUNT);
        BigDecimal splitCouponAmount = jsonObj.getBigDecimal(Constant.SPLIT_ACTIVITY_AMOUNT);
        BigDecimal splitActivityAmount = jsonObj.getBigDecimal(Constant.SPLIT_COUPON_AMOUNT);
        BigDecimal splitTotalAmount = jsonObj.getBigDecimal(Constant.SPLIT_TOTAL_AMOUNT);
        Long ts = jsonObj.getLong("ts") * 1000;

        return TradeSkuOrderStatistics.builder()
                .skuId(skuId)
                .originalAmount(splitOriginalAmount)
                .couponReduceAmount(splitCouponAmount)
                .activityReduceAmount(splitActivityAmount)
                .orderAmount(splitTotalAmount)
                .ts(ts)
                .build();
    }

}
