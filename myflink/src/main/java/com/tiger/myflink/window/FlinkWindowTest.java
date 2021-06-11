package com.tiger.myflink.window;

import com.tiger.myflink.TempSensorData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

/**
 * @Author Zenghu
 * @Date 2021/5/26 22:30
 * @Description
 * @Version: 1.0
 **/
public class FlinkWindowTest {

    private static String topic = "sensor_data";

    private static Properties properties = new Properties();
    private static Schema schema;

    static {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092");
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

        //env.getConfig().setAutoWatermarkInterval(200);

        DataStream<GenericRecord> dataStream = env.addSource(new FlinkKafkaConsumer<>(topic, AvroDeserializationSchema.forGeneric(schema), properties));


        DataStream<TempSensorData> sensorDataDataStream = dataStream.map((MapFunction<GenericRecord, TempSensorData>) value -> new TempSensorData(value.get("id").toString(), (long) value.get("time"), (double) value.get("temp")));


        // 温度数据累加
        // 时间滚动窗口
        sensorDataDataStream.keyBy(TempSensorData::getId).timeWindow(Time.seconds(10))
                .reduce((ReduceFunction<TempSensorData>) (value1, value2) -> {
                    TempSensorData result = new TempSensorData();
                    result.setId(value1.getId());
                    result.setTime(System.currentTimeMillis() / 1000L);
                    result.setTemp(value1.getTemp() + value2.getTemp());
                    return result;
                }).print("sum");

        SingleOutputStreamOperator<TempSensorData> aggregateDataStream = sensorDataDataStream.keyBy(TempSensorData::getId).timeWindow(Time.seconds(10))
                .aggregate(new TempSensorDataAggregateFunction());

        aggregateDataStream.print("avg");

        // 滑动窗口
        sensorDataDataStream.keyBy(TempSensorData::getId).timeWindow(Time.seconds(10), Time.seconds(5))
                .aggregate(new TempSensorDataAggregateFunction())
                .print("slid");




        env.execute();
    }

    private static class TempSensorDataAggregateFunction implements AggregateFunction<TempSensorData, Tuple3<String, Integer, Double>, TempSensorData> {
        @Override
        public Tuple3<String, Integer, Double> createAccumulator() {
            return new Tuple3<>("", 0, 0D);
        }

        @Override
        public Tuple3<String, Integer, Double> add(TempSensorData value, Tuple3<String, Integer, Double> accumulator) {
            accumulator.f0 = value.getId();
            accumulator.f1 += 1;
            accumulator.f2 += value.getTemp();
            return accumulator;
        }

        @Override
        public TempSensorData getResult(Tuple3<String, Integer, Double> accumulator) {
            TempSensorData result = new TempSensorData();
            result.setId(accumulator.f0);
            result.setTime(accumulator.f1); // 将时间设置为计数值
            result.setTemp(accumulator.f2 / accumulator.f1);

            return result;
        }

        @Override
        public Tuple3<String, Integer, Double> merge(Tuple3<String, Integer, Double> a, Tuple3<String, Integer, Double> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
