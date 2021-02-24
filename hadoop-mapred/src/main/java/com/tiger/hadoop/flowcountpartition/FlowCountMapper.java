package com.tiger.hadoop.flowcountpartition;

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
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("[\\s\\t]+");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpPakcges(Integer.parseInt(split[split.length - 5]));
        flowBean.setDownPackages(Integer.parseInt(split[split.length - 4]));
        flowBean.setUpFlow(Integer.parseInt(split[split.length - 3]));
        flowBean.setDownFlow(Integer.parseInt(split[split.length - 2]));
        context.write(new Text(split[1]), flowBean);
    }
}
