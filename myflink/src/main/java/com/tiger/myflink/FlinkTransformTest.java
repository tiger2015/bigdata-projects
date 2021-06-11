package com.tiger.myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Zenghu
 * @Date 2021/5/24 22:38
 * @Description
 * @Version: 1.0
 **/
public class FlinkTransformTest {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.readTextFile("data/sensor.txt");


        // map
        DataStream<Integer> mapDataStream = dataStream.map((MapFunction<String, Integer>) value -> value.length());
        mapDataStream.print();

        // flatMap

       DataStream<String> flatMapDataStream =  dataStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
           String[] split = value.split(",");
           for(String sub:split){
               out.collect(sub);
           }
       }).returns(String.class);

       flatMapDataStream.print("flatMap");

       // filter

        DataStream<String> filterDataStream = dataStream.filter((FilterFunction<String>) value -> value.startsWith("sensor_1"));

        filterDataStream.print("filter");


        // keyBy Max

        dataStream.map((MapFunction<String, TempSensorData>) value -> {
            String[] split = value.split(",");
            return new TempSensorData(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }).keyBy(TempSensorData::getId).max("temp").print("max");



        env.execute();

    }


}
