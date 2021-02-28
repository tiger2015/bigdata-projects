package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 22:36
 * @Description
 * @Version: 1.0
 **/
public class ScoreAvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    /*
      输入： K1       K2
          偏移量     computer,huangxiaoming,85,86,41,75,93,42,85
      输出： K2                        V2
            computer,huangxiaoming     85
            computer,huangxiaoming     86
            computer,huangxiaoming     41
            computer,huangxiaoming     75
            computer,huangxiaoming     93
            computer,huangxiaoming     42
            computer,huangxiaoming     85
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        for (int i = 2; i < split.length; i++) {
            context.write(new Text(split[0] + "," + split[1]), new DoubleWritable(Double.parseDouble(split[i])));
        }


    }
}
