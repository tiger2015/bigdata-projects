package com.tiger.myflink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

/**
 * @Author Zenghu
 * @Date 2021/5/24 21:18
 * @Description
 * @Version: 1.0
 **/
public class SensorDataKafkaSource {

    private static String topic = "sensor_data";

    private static Properties properties = new Properties();
    private static Schema schema;

    static {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.201:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        try {
            URL schemaURL = Thread.currentThread().getContextClassLoader().getResource("TempSensor.avsc");
            String decode = URLDecoder.decode(schemaURL.toString(), "UTF-8");
            File file = new File(decode.substring(6));
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<GenericRecord> dataStream = env.addSource(new FlinkKafkaConsumer<>(topic, AvroDeserializationSchema.forGeneric(schema), properties));


        DataStream<TempSensorData> sensorDataDataStream = dataStream.map((MapFunction<GenericRecord, TempSensorData>) value -> new TempSensorData(value.get("id").toString(), (long) value.get("time"), (double) value.get("temp")));


        sensorDataDataStream.keyBy("id").maxBy("temp").print();


        env.execute();
    }
}
