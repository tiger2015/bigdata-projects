package com.tiger.hadoop.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/23 20:53
 * @Description
 * @Version: 1.0
 **/
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("[\\s\\t]+");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpPakcges(Integer.parseInt(split[1]));
        flowBean.setDownPackages(Integer.parseInt(split[2]));
        flowBean.setUpFlow(Integer.parseInt(split[3]));
        flowBean.setDownFlow(Integer.parseInt(split[4]));
        context.write(flowBean, new Text(split[0]));
    }
}
