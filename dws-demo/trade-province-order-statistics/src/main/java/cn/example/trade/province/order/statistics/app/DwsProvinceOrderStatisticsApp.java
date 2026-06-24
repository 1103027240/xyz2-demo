package cn.example.trade.province.order.statistics.app;

import cn.example.common.demo.base.BaseApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.entity.TradeProvinceOrderStatistics;
import cn.example.common.demo.function.BeanToJsonFunction;
import cn.example.common.demo.utils.FlinkSinkUtil;
import cn.example.trade.province.order.statistics.function.*;
import cn.example.trade.province.order.statistics.function.dimension.ProvinceAsyncFunction;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsProvinceOrderStatisticsApp extends BaseApp {

    public DwsProvinceOrderStatisticsApp() {
        super(Constant.DWS_PROVINCE_ORDER_STATISTICS_SERVER_PORT, Constant.PARALLELISM, Constant.DWD_ORDER_DETAIL, Constant.DWS_TRADE_PROVINCE_ORDER_STATISTICS);
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
        new DwsProvinceOrderStatisticsApp().run();
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1、过滤空值、左外连接数据先来后到
        SingleOutputStreamOperator<JSONObject> distinctDS = kafkaStrDS
                .process(new ParameterFilterMapFunction())
                .keyBy(jsonObj -> jsonObj.getString("id"))
                .process(new LeftJoinDistinctKeyedProcessFunction());

        // 2、设置水位线
        SingleOutputStreamOperator<JSONObject> watermarkDS = distinctDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(Constant.WATERMARK_DELAY))
                .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000)
                .withIdleness(Duration.ofSeconds(Constant.WATERMARK_IDLE_TIMEOUT)));

        // 3、分组、开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderStatistics> reduceDS = watermarkDS
                .map(new ProvinceOrderStatisticsMapFunction())
                .keyBy(TradeProvinceOrderStatistics::getProvinceId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(Constant.WINDOW_SIZE)))
                .allowedLateness(Time.seconds(Constant.WATERMARK_OUT_OF_ORDER))
                .reduce(new ProvinceOrderStatisticsReduceFunction(), new ProvinceOrderStatisticsWindowFunction());

        // 4、关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderStatistics> provinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new ProvinceAsyncFunction(),
                60,
                TimeUnit.SECONDS);

        // 5、存储到StarRocks
        provinceDS
                .map(new BeanToJsonFunction<>())
                .addSink(FlinkSinkUtil.createStarRocksSink(Constant.DATABASE_DWS, Constant.DWS_TRADE_PROVINCE_ORDER_STATISTICS));

    }

}
