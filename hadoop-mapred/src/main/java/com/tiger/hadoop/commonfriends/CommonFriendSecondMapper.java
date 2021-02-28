package com.tiger.hadoop.commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author Zenghu
 * @Date 2021/2/27 22:19
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendSecondMapper extends Mapper<LongWritable, Text, Text, Text> {

    /*
       输入： K1       V1
             偏移量    B   A,E,F,J   # B的好友
       输出： K2       V2
             A,E     B  # A和E的共同好友B
             A,F     B
             A,J     B
             E,F     B
             E,J     B
             F,J     B
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String user = split[0];
        String[] friends = split[1].split(",");
        // 排序操作，避免 A-B 和 B-A
        Arrays.sort(friends);
        for (int i = 0; i < friends.length - 1; i++) {
            for (int j = i+1; j < friends.length; j++) {
                context.write(new Text(friends[i]+"-"+friends[j]), new Text(user));
            }
        }
    }
}
