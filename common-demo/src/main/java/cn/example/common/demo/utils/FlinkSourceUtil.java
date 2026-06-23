package cn.example.common.demo.utils;

import cn.example.common.demo.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                // 优先从Checkpoint位点恢复，无位点则从最新开始offset消费（生产环境）
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 从最新offset开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                // Kafka事务隔离级别：读已提交
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, new String(IsolationLevel.READ_COMMITTED.toString()))
                // 关闭Kafka原生自动提交，靠Checkpoint提交offset
                //.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .setValueOnlyDeserializer(
                        new DeserializationSchema<>() {
                            @Override
                            public String deserialize(byte[] message) {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
    }

    public static MySqlSource<String> getMySqlSource(String database, String tableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database + "." + tableName)
                .username(Constant.MYSQL_USERNAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
    }

}
