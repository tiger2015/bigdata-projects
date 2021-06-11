package com.tiger.myflink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Zenghu
 * @Date 2021/5/31 22:00
 * @Description
 * @Version: 1.0
 **/
public class FlinkKeyedStateTest {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.100.4", 7777);


        SingleOutputStreamOperator<String> wordsStream = socketTextStream.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String sub : s) {
                    out.collect(sub.toLowerCase());
                }
            }
        });

        wordsStream
                .keyBy(String::toString)
                .map(new WordCountMappper())
                .print();


        env.execute();
    }


    public static class WordCountMappper extends RichMapFunction<String, Tuple2<String, Integer>> {

        private MapState<String, Integer> myMapState;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义映射状态
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("wordcount", String.class, Integer.class));

            //getRuntimeContext().getListState();
            //getRuntimeContext().getAggregatingState();
            //getRuntimeContext().getReducingState();

        }

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {

            Integer count = myMapState.get(value);
            if (count == null) {
                count = 0;
            }
            count++;
            myMapState.put(value, count);
            return new Tuple2<>(value, count);
        }
    }
}
