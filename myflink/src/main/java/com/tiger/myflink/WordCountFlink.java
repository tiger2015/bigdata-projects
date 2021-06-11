package com.tiger.myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author Zenghu
 * @Date 2021/5/22 16:29
 * @Description
 * @Version: 1.0
 **/
public class WordCountFlink {


    public static void main(String[] args) throws Exception {

        // TODO 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<String> dataSet = env.readTextFile("data/words");


        DataSet<Tuple2<String, Integer>> result = dataSet
                .flatMap(new WordFlatMapFunction()).setParallelism(2)
                .groupBy(0)
                .sum(1).setParallelism(2);

        result.print();

    }


    public static class WordFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split("\\s+");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
