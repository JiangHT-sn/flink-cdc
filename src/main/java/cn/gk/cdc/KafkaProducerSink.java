package cn.gk.cdc;

import cn.gk.pojo.AfterBean;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author：HT
 * @Date：2022/6/23
 * @Description： flink-kafka-sink 封装类
 */
public class KafkaProducerSink {
    /**
     * 1.获取 getStringFlinkKafkaProducer
     */
    public static FlinkKafkaProducer<String> getStringFlinkKafkaProducer(String topics, Properties properties) {
        return new FlinkKafkaProducer<>(
                topics,                                    // 主题 topic
                new CustomKafkaSerialization.StringCustomKafkaSerialization(topics), // serialization schema
                properties,                                // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 设置语义
    }

    /**
     * 2.获取 getPojoFlinkKafkaProducer
     */
    public static FlinkKafkaProducer<AfterBean> getPojoFlinkKafkaProducer(String topics, Properties properties) {
        return new FlinkKafkaProducer<>(
                topics,                                    // target topic
                new CustomKafkaSerialization.PojoCustomKafkaSerialization(topics), // serialization schema
                properties,                                // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 设置语义
    }
}
