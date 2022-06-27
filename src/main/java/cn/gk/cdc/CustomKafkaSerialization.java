package cn.gk.cdc;

import cn.gk.pojo.AfterBean;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @Author：HT
 * @Date：2022/6/22
 * @Description： 实现kafka的序列化器
 */
/*
 @Override
 public void open(SerializationSchema.InitializationContext context) throws Exception {
    // 获取度量对象
    MetricGroup metricGroup = context.getMetricGroup();
    // 获取用户类加载器
    UserCodeClassLoader userCodeClassLoader = context.getUserCodeClassLoader();
 }
 */
class CustomKafkaSerialization {

    /**
     * string 类型的数据写入kafka
     */
    static class StringCustomKafkaSerialization implements KafkaSerializationSchema<String> {
        private String topic;

        public StringCustomKafkaSerialization(String topic) {
            this.topic = topic;
        }


        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * pojo类型的数据写入kafka
     */
    static class PojoCustomKafkaSerialization implements KafkaSerializationSchema<AfterBean> {
        private String topic;
        private ObjectMapper objectMapper;

        public PojoCustomKafkaSerialization(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(AfterBean element, @Nullable Long timestamp) {
            // 接受数据使用
            byte[] b = null;
            if (objectMapper == null) objectMapper = new ObjectMapper();

            try {
                // 处理对象类型的数据
                b = objectMapper.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                // 记录出错记录
            }
            return new ProducerRecord<>(topic, b);
        }
    }

}
