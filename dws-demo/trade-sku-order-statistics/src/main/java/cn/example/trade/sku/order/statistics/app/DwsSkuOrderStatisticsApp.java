package cn.example.trade.sku.order.statistics.app;

import cn.example.common.demo.base.BaseApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TradeSkuOrderStatistics;
import cn.example.common.demo.function.BeanToJsonFunction;
import cn.example.common.demo.utils.FlinkSinkUtil;
import cn.example.trade.sku.order.statistics.function.*;
import cn.example.trade.sku.order.statistics.function.dimension.*;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsSkuOrderStatisticsApp extends BaseApp {

    public DwsSkuOrderStatisticsApp() {
        super(Constant.DWS_SKU_ORDER_STATISTICS_SERVER_PORT, Constant.PARALLELISM, Constant.DWD_ORDER_DETAIL, Constant.DWS_TRADE_SKU_ORDER_STATISTICS);
    }

    /**
     * 启动参数
     * --add-opens java.base/java.nio=ALL-UNNAMED
     * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
     * --add-opens java.base/java.lang=ALL-UNNAMED
     * --add-opens java.base/java.util=ALL-UNNAMED
     * 建表语句字符串用VARCHAR，指定字符串大小
     */
    public static void main(String[] args) throws Exception {
        new DwsSkuOrderStatisticsApp().run();
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        // 1、过滤空值、左外连接数据先来后到
        SingleOutputStreamOperator<JSONObject> distinctDS = kafkaDS
                .process(new ParameterFilterMapFunction())
                .keyBy(e -> e.getString("id"))
                .process(new LeftJoinDistinctKeyedProcessFunction());

        // 2、设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkDS = distinctDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(Constant.WATERMARK_DELAY))
                .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000)
                .withIdleness(Duration.ofSeconds(Constant.WATERMARK_IDLE_TIMEOUT)));

        // 3、分组、开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderStatistics> reduceDS = watermarkDS
                .map(new SkuOrderStatisticsMapFunction())
                .keyBy(TradeSkuOrderStatistics::getSkuId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(Constant.WINDOW_SIZE)))
                .allowedLateness(Time.seconds(Constant.WATERMARK_OUT_OF_ORDER))
                .reduce(new SkuOrderStatisticsReduceFunction(), new SkuOrderStatisticsWindowFunction());

        // 4、关联SKU维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> skuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new SkuInfoAsyncFunction(),
                60,
                TimeUnit.SECONDS);

        // 5、关联SPU维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> spuInfoDS = AsyncDataStream.unorderedWait(
                skuInfoDS,
                new SpuInfoAsyncFunction(),
                60,
                TimeUnit.SECONDS);

        // 6、关联品牌维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> tradeMarkDS = AsyncDataStream.unorderedWait(
                spuInfoDS,
                new TradeMarkAsyncFunction(),
                60,
                TimeUnit.SECONDS);

        // 7、关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> category3DS = AsyncDataStream.unorderedWait(
                tradeMarkDS,
                new Category3AsyncFunction(),
                120,
                TimeUnit.SECONDS);

        // 8、关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> category2DS = AsyncDataStream.unorderedWait(
                category3DS,
                new Category2AsyncFunction(),
                120,
                TimeUnit.SECONDS);

        // 9、关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderStatistics> category1DS = AsyncDataStream.unorderedWait(
                category2DS,
                new Category1AsyncFunction(),
                120,
                TimeUnit.SECONDS);

        // 10、存储到StarRocks
        category1DS.map(new BeanToJsonFunction<>())
                .addSink(FlinkSinkUtil.createStarRocksSink(Constant.DATABASE_DWS, Constant.DWS_TRADE_SKU_ORDER_STATISTICS));

    }

}
