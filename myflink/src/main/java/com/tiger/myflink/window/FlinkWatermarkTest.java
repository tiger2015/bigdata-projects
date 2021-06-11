package com.tiger.myflink.window;

import com.tiger.myflink.TempSensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @Author Zenghu
 * @Date 2021/5/29 15:51
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class FlinkWatermarkTest {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2);

        // 设置时间语义为时间时间，默认时采用摄入时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置watermark时间间隔
        env.getConfig().setAutoWatermarkInterval(100L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, org.apache.flink.api.common.time.Time.seconds(3)));

        // 从网络读取数据
        DataStream<String> socketStream = env.socketTextStream("192.168.100.4", 7777);

        // 设置watermark
        DataStream<TempSensorData> watermarkDataStream = socketStream
                .map(new MapFunction<String, TempSensorData>() {
                    @Override
                    public TempSensorData map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TempSensorData>(Time.seconds(2)) {

                    @Override
                    public long extractTimestamp(TempSensorData element) {
                        log.info("data:{}, watermark:{}", element.getTime(), getCurrentWatermark().getTimestamp() / 1000);
                        return element.getTime() * 1000L;
                    }
                }).setParallelism(1);
        //watermarkDataStream.print();

        // 侧输入流
        // 注意如果不指定类型，则需要使用匿名类来实现，否则会出现Could not determine TypeInformation for the OutputTag type异常
        OutputTag<TempSensorData> dataOutputTag = new OutputTag<TempSensorData>("late", TypeInformation.of(TempSensorData.class));

        SingleOutputStreamOperator<TempSensorData> minDataStream = watermarkDataStream
                .keyBy("id")
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.minutes(1)) // 允许10秒后关闭窗口
                .sideOutputLateData(dataOutputTag)
                .minBy("temp");

        minDataStream.print("min");
        minDataStream.getSideOutput(dataOutputTag).print("late");

        env.execute();
    }


}
