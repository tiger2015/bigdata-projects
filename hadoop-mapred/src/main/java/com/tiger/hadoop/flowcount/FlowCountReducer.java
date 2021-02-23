package com.tiger.hadoop.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/23 20:58
 * @Description
 * @Version: 1.0
 **/
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upPackages = 0;
        int downPackages = 0;
        int upFlow = 0;
        int downFlow = 0;
        for (FlowBean value : values) {
            upPackages += value.getUpPakcges();
            downPackages += value.getDownPackages();
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        FlowBean flowBean = new FlowBean();
        flowBean.setUpPakcges(upPackages);
        flowBean.setDownPackages(downPackages);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        context.write(key, flowBean);
    }
}
