package com.tiger.hadoop.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/23 20:58
 * @Description
 * @Version: 1.0
 **/
public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString() + "\t");
        }
        context.write(new Text(builder.toString()), key);
    }
}
