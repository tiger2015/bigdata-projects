package com.tiger.myflink.common;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

/**
 * @Author Zenghu
 * @Date 2021/6/5 22:53
 * @Description
 * @Version: 1.0
 **/
public class FlinkSourceStreamUtil {


    private static Properties properties;
    private static Schema schema;

    static {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("TempSensor.avsc");
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    // 从kafka读取数据
    public static FlinkKafkaConsumerBase<String> createKafkaSourceStream(String topic) {
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }

    // 从socket读取数据
    public static SocketTextStreamFunction createTextSocketStream(String ip, int port) {
        return new SocketTextStreamFunction(ip, port, "\n", 0);
    }

    // 使用avro的数据
    public static FlinkKafkaConsumerBase<GenericRecord> createKafkaSourceStreamUseAvro(String topic) {
        return new FlinkKafkaConsumer<>(topic, AvroDeserializationSchema.forGeneric(schema), properties);
    }

}


