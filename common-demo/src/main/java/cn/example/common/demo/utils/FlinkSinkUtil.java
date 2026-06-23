package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkSinkUtil {

    public static SinkFunction<String> createStarRocksSink(String databaseName, String tableName) {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", Constant.STARROCKS_JDBC_URL)
                .withProperty("load-url", Constant.STARROCKS_LOAD_URL)
                .withProperty("database-name", databaseName)
                .withProperty("table-name", tableName)
                .withProperty("username", Constant.STARROCKS_USERNAME)
                .withProperty("password", Constant.STARROCKS_PASSWORD)
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.write_mode", Constant.STARROCKS_SINK_WRITE_MODE)
                .withProperty("sink.semantic", Constant.STARROCKS_SINK_SEMANTIC)
                .withProperty("sink.max-retries", Constant.STARROCKS_SINK_MAX_RETRIES)
                .withProperty("sink.buffer-flush.max-rows", Constant.STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS)
                .withProperty("sink.buffer-flush.max-bytes", Constant.STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES)
                .withProperty("sink.buffer-flush.interval-ms", Constant.STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS)
                .build();
        return StarRocksSink.sink(sinkOptions);
    }

}
