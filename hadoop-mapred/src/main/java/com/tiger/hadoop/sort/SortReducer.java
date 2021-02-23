package com.tiger.hadoop.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/21 21:28
 * @Description
 * @Version: 1.0
 **/
public class SortReducer extends Reducer<PairWritable, NullWritable, PairWritable, NullWritable> {

    @Override
    protected void reduce(PairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
