package com.tiger.myflink.window;

import com.tiger.myflink.TempSensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Zenghu
 * @Date 2021/6/1 21:46
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class SensorTemperatureMonitor {

    private static String topic = "sensor_data";

    private static Properties properties;

    static {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.201:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(4);

        FlinkKafkaConsumer<String> kafkaConsumer010 = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStream = env.addSource(kafkaConsumer010);

        kafkaConsumer010.setStartFromLatest();

        SingleOutputStreamOperator<TempSensorData> sensorDataStream = stringDataStream.map(new MapFunction<String, TempSensorData>() {
            @Override
            public TempSensorData map(String value) throws Exception {
                String[] split = value.split(",");
              //  log.info("sensor:{}", value);
                return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).setParallelism(5);


        SingleOutputStreamOperator<TempSensorData> watermarkDataStream = sensorDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TempSensorData>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(TempSensorData element) {
                log.info(">>> sensor:{}, timestamp:{}, watermark:{}", element.getId(), element.getTime(), getCurrentWatermark().getTimestamp() / 1000L);
                return element.getTime() * 1000L;
            }
        }).setParallelism(4)
                .process(new ProcessFunction<TempSensorData, TempSensorData>() {
                    @Override
                    public void processElement(TempSensorData value, Context ctx, Collector<TempSensorData> out) throws Exception {

                      //  ctx.timerService().currentWatermark();

                      //  log.info("sensor:{}, timestamp:{}, watermark:{}", value.getId(), value.getTime(), ctx.timerService().currentWatermark() / 1000L);
                        out.collect(value);
                    }
                });

        SingleOutputStreamOperator<TempSensorData> minTempDataStream = watermarkDataStream
                .keyBy(TempSensorData::getId)
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.seconds(15))
                .minBy("temp");

        minTempDataStream.addSink(new SinkFunction<TempSensorData>() {
            @Override
            public void invoke(TempSensorData value, Context context) throws Exception {
                log.info("min temp:{}", value.toString());
            }
        });

        env.execute();
    }
}
