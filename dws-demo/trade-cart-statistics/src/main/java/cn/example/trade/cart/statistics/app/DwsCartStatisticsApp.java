package cn.example.trade.cart.statistics.app;

import cn.example.common.demo.base.BaseApp;
import cn.example.common.demo.constant.Constant;
import cn.example.common.demo.function.BeanToJsonFunction;
import cn.example.common.demo.utils.FlinkSinkUtil;
import cn.example.trade.cart.statistics.function.CartAggregateFunction;
import cn.example.trade.cart.statistics.function.CartWindowFunction;
import cn.example.trade.cart.statistics.function.CartKeyedProcessFunction;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class DwsCartStatisticsApp extends BaseApp {

    public DwsCartStatisticsApp(boolean isClusterMode) {
        super(Constant.DWS_CART_STATISTICS_SERVER_PORT, Constant.PARALLELISM, Constant.DWD_CART_ADD, Constant.DWS_CART_STATISTICS, isClusterMode);
    }

    public static void main(String[] args) throws Exception {
        new DwsCartStatisticsApp(Constant.IS_CLUSTER_MODE).run();
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(Constant.WATERMARK_DELAY))
                        .withTimestampAssigner((jsonObj, recordTimestamp) -> jsonObj.getLong("ts") * 1000)
                        .withIdleness(Duration.ofSeconds(Constant.WATERMARK_IDLE_TIMEOUT)));

        // 分组过滤独立加购用户、开窗聚合、存储到StarRocks
        jsonObjDS
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .process(new CartKeyedProcessFunction())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(Constant.WINDOW_SIZE)))
                .allowedLateness(Time.seconds(Constant.WATERMARK_OUT_OF_ORDER))
                .aggregate(new CartAggregateFunction(), new CartWindowFunction())
                .map(new BeanToJsonFunction())
                .addSink(FlinkSinkUtil.createStarRocksSink(Constant.DATABASE_DWS, Constant.DWS_CART_STATISTICS));
    }

}
