package com.tiger.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/17 23:55
 * @Description
 * @Version: 1.0
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (IntWritable value : values) {
            count += value.get();

        }
        context.getCounter(CounterEnum.REDUCE_RECORDS).increment(1L);
        context.write(key, new LongWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
