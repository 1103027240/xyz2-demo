package cn.example.trade.sku.order.statistics.function;

import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class SkuOrderStatisticsWindowFunction extends ProcessWindowFunction<TradeSkuOrderStatistics, TradeSkuOrderStatistics, String, TimeWindow> {

    @Override
    public void process(String skuId, Context ctx, Iterable<TradeSkuOrderStatistics> iterable, Collector<TradeSkuOrderStatistics> out) throws Exception {
        TradeSkuOrderStatistics skuOrderStatistics = iterable.iterator().next();
        TimeWindow window = ctx.window();
        skuOrderStatistics.setWindowStart(new Date(window.getStart()));
        skuOrderStatistics.setWindowEnd(new Date(window.getEnd()));
        skuOrderStatistics.setDt(new Date(window.getStart()));
        out.collect(skuOrderStatistics);
    }

}
