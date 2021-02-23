package com.tiger.hadoop.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/21 21:26
 * @Description
 * @Version: 1.0
 **/
public class SortMapper extends Mapper<LongWritable, Text, PairWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(",");
        PairWritable pairWritable = new PairWritable();
        pairWritable.setWord(split[0]);
        pairWritable.setScore(Double.parseDouble(split[1]));
        context.write(pairWritable, NullWritable.get());
    }
}
