package com.tiger.myflink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Zenghu
 * @Date 2021/5/22 16:45
 * @Description
 * @Version: 1.0
 **/
public class WordCountFlinkStream {


    public static void main(String[] args) throws Exception {

        // TODO 创建流执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String host = parameterTool.get("host", "localhost");
        int port = parameterTool.getInt("port", 9000);

        // TODO 设置并行度
        DataStream<String> stream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> result = stream
                .flatMap(new WordCountFlink.WordFlatMapFunction()).setParallelism(2)
                .keyBy(0)
                .sum(1).setParallelism(3);

        result.print().setParallelism(1);

        env.execute();
    }

}
