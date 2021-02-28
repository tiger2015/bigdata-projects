package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 22:41
 * @Description
 * @Version: 1.0
 **/
public class ScoreAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }
        context.write(key, new DoubleWritable(sum / count));
    }
}
