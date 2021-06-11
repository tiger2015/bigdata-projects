package com.tiger.myflink.processfun;

import com.tiger.myflink.TempSensorData;
import com.tiger.myflink.common.FlinkSourceStreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author Zenghu
 * @Date 2021/6/5 22:45
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class ProcessFuncSideOuputTest {

    private static String topic = "sensor_data";


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setStateBackend((StateBackend) new MemoryStateBackend());


        env.enableCheckpointing(200);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(4);

        DataStreamSource<String> stringDataStream = env.addSource(FlinkSourceStreamUtil.createKafkaSourceStream(topic));

        // 解析温度数据
        SingleOutputStreamOperator<TempSensorData> sensorDataStream = stringDataStream.map(new MapFunction<String, TempSensorData>() {
            @Override
            public TempSensorData map(String value) throws Exception {
                String[] split = value.split(",");
                return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).setParallelism(5);


        //
        OutputTag<TempSensorData> lowTempStreamTag = new OutputTag<TempSensorData>("low_temp") {
        };


        SingleOutputStreamOperator<TempSensorData> highTempStream = sensorDataStream
                .process(new ProcessFunction<TempSensorData, TempSensorData>() {
                    @Override
                    public void processElement(TempSensorData value, Context ctx, Collector<TempSensorData> out) throws Exception {
                        if (value.getTemp() > 30) {
                            out.collect(value);
                        } else {
                            ctx.output(lowTempStreamTag, value);
                        }
                    }
                });

        DataStream<TempSensorData> lowTempStream = highTempStream.getSideOutput(lowTempStreamTag);

        highTempStream.addSink(new SinkFunction<TempSensorData>() {
            @Override
            public void invoke(TempSensorData value, Context context) throws Exception {
                log.info("high:{}", value);
            }
        });


        lowTempStream.addSink(new SinkFunction<TempSensorData>() {
            @Override
            public void invoke(TempSensorData value, Context context) throws Exception {
                log.info("low:{}", value);
            }
        });

        env.execute();
    }


}
