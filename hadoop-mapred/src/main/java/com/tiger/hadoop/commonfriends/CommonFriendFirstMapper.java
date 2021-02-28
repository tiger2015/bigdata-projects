package com.tiger.hadoop.commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/27 21:30
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
                        V1
           输入:       A:B,C,D,F,E,O
           输出： K2    V2
                 B     A
                 C     A
                 D     A
                 F     A
                 E     A
                 O     A
        */
        String[] split = value.toString().split(":");
        String user = split[0];
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            context.write(new Text(friend), new Text(user));
        }
    }
}
