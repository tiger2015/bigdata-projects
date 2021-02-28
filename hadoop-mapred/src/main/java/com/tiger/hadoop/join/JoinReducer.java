package com.tiger.hadoop.join;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author Zenghu
 * @Date 2021/2/27 11:20
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String productId = key.toString();
        String productInfo = "";
        List<String> orderList = new ArrayList<>();
        for (Text value : values) {
            if (value.toString().startsWith(productId)) {
                productInfo = value.toString();
            } else {
                orderList.add(value.toString());
            }
        }
        for (String value : orderList) {
            context.write(new Text(value + "," + productInfo), NullWritable.get());
        }
    }
}
