package com.tiger.myflink.state;

import com.tiger.myflink.TempSensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

/**
 * @Author Zenghu
 * @Date 2021/5/31 21:21
 * @Description
 * @Version: 1.0
 **/
public class FlinkListStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2);

        // 设置时间语义为时间时间，默认时采用摄入时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置watermark时间间隔
        env.getConfig().setAutoWatermarkInterval(100L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, org.apache.flink.api.common.time.Time.seconds(3)));

        env.setParallelism(2);

        // 从网络读取数据
        DataStream<String> socketStream = env.socketTextStream("192.168.100.4", 7777);


        SingleOutputStreamOperator<TempSensorData> tempSensorDataDataStream = socketStream.map(new MapFunction<String, TempSensorData>() {
            @Override
            public TempSensorData map(String value) throws Exception {
                String[] split = value.split(",");
                return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        tempSensorDataDataStream.map(new CounterMap())
                .print();

        env.execute();
    }


    public static class CounterMap extends RichMapFunction<TempSensorData, Tuple2<String, Integer>> implements ListCheckpointed<Integer> {
        private Integer count ;  // 每个task内部共享一个变量, 当操作算子的并行度发生变化时，状态会重新分布

        @Override
        public void open(Configuration parameters) throws Exception {
            count = 0;
        }

        @Override
        public Tuple2<String, Integer> map(TempSensorData value) throws Exception {
            count++;
            return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask()+"", count);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer l : state) {
                count += l;
            }
        }
    }


}
