package cn.example.trade.cart.statistics.function;

import cn.example.common.demo.entity.CartStatistics;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Date;

public class CartWindowFunction implements AllWindowFunction<Long, CartStatistics, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<Long> iterable, Collector<CartStatistics> out) throws Exception {
        CartStatistics result = CartStatistics.builder()
                .windowStart(new Date(window.getStart()))
                .windowEnd(new Date(window.getEnd()))
                .dt(new Date(window.getStart()))
                .cartAddCnt(iterable.iterator().next())
                .build();
        out.collect(result);
    }

}
