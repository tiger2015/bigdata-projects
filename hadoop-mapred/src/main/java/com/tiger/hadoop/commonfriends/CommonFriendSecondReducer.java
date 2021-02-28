package com.tiger.hadoop.commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author Zenghu
 * @Date 2021/2/27 22:25
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendSecondReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer commonFriends = new StringBuffer();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            Text next = iterator.next();
            if(iterator.hasNext()){
                commonFriends.append(next).append(",");
            }else {
                commonFriends.append(next);
            }
        }
        context.write(key, new Text(commonFriends.toString()));
    }
}
