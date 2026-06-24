package cn.example.trade.province.order.statistics.function;

import cn.example.common.demo.entity.TradeProvinceOrderStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class ProvinceOrderStatisticsWindowFunction implements WindowFunction<TradeProvinceOrderStatistics, TradeProvinceOrderStatistics, String, TimeWindow> {

    @Override
    public void apply(String provinceId, TimeWindow window, Iterable<TradeProvinceOrderStatistics> iterable, Collector<TradeProvinceOrderStatistics> out) throws Exception {
        TradeProvinceOrderStatistics provinceOrderStatistics = iterable.iterator().next();
        provinceOrderStatistics.setWindowStart(new Date(window.getStart()));
        provinceOrderStatistics.setWindowEnd(new Date(window.getEnd()));
        provinceOrderStatistics.setDt(new Date(window.getStart()));
        provinceOrderStatistics.setOrderCount((long) provinceOrderStatistics.getOrderIdSet().size());
        out.collect(provinceOrderStatistics);
    }

}
