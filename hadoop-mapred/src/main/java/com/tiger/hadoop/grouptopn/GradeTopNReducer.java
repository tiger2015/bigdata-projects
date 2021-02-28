package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 23:00
 * @Description
 * @Version: 1.0
 **/
public class GradeTopNReducer extends Reducer<Grade, Text, Text, NullWritable> {
    private int n;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = context.getConfiguration().getInt("top.n", 3);
    }

    @Override
    protected void reduce(Grade key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text value : values) {
            if (i >= n) break;
            context.write(value, NullWritable.get());
            i++;
        }
        System.out.println("i="+i);
    }
}
