package com.tiger.hadoop.flowcountpartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author Zenghu
 * @Date 2021/2/24 22:34
 * @Description
 * @Version: 1.0
 **/
public class FlowCountPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNum = text.toString();
        if(phoneNum.startsWith("135")){
            return 0;
        }else if(phoneNum.startsWith("136")){
            return 1;
        }else if (phoneNum.startsWith("137")){
            return 2;
        }else {
            return 3;
        }

    }
}
