package com.tiger.myflink.processfun;

import com.tiger.myflink.TempSensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author Zenghu
 * @Date 2021/6/5 21:24
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class SensorTemperatureConIncreaseMonitor {
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

        //env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(4);

        FlinkKafkaConsumer<String> kafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStream = env.addSource(kafkaConsumer010);

        // 解析温度数据
        SingleOutputStreamOperator<TempSensorData> sensorDataStream = stringDataStream.map(new MapFunction<String, TempSensorData>() {
            @Override
            public TempSensorData map(String value) throws Exception {
                String[] split = value.split(",");
                return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).setParallelism(5);


        SingleOutputStreamOperator<TempSensorData> watermarkStream = sensorDataStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TempSensorData>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(TempSensorData element) {
                        return element.getTime() * 1000L;
                    }
                }).setParallelism(4);


        watermarkStream.keyBy(TempSensorData::getId)
                .process(new SensorTempIncreaseWarning(10));


        env.execute();


    }


    public static class SensorTempIncreaseWarning extends KeyedProcessFunction<String, TempSensorData, TempSensorData> {
        private long time;

        private ValueState<Double> prevTemp;

        private ValueState<Long> prevTriggerTime;

        public SensorTempIncreaseWarning(long time) {
            this.time = time;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            prevTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp", Double.class));
            prevTriggerTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("trigger", Long.class));
        }

        @Override
        public void processElement(TempSensorData value, Context ctx, Collector<TempSensorData> out) throws Exception {
            // 判断温度是否连续上升
            Double prev = prevTemp.value();
            if (prev != null) {
                // 如果温度连续上升,且位注册定时任务
                if (value.getTemp() > prev && prevTriggerTime.value() == null) {
                    ctx.timerService().registerEventTimeTimer((value.getTime() + time) * 1000L);
                    prevTriggerTime.update((value.getTime() + time) * 1000L);
                    log.info("add trigger");
                } else if (value.getTemp() <= prev && prevTriggerTime.value() != null) {
                    // 温度出现下降,需要取消定时任务
                    ctx.timerService().deleteEventTimeTimer(prevTriggerTime.value());
                    prevTriggerTime.clear();
                    log.info("remove trigger");
                }
            }
            prevTemp.update(value.getTemp());

            out.collect(value);
        }

        // 定时任务触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TempSensorData> out) throws Exception {
            log.warn("sensor:{} continue increase", ctx.getCurrentKey());
            prevTriggerTime.clear();
        }

        @Override
        public void close() throws Exception {
            prevTriggerTime.clear();
            prevTemp.clear();
        }
    }
}
