package com.tiger.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/17 23:48
 * @Description
 * @Version: 1.0
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(CounterEnum.MAP_RECORDS).increment(1L);
        String s = value.toString();
        String[] split = s.split("[\\s+|,|.]");
        for (String word : split) {
            if (word.length() == 0) continue;
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
