package com.tiger.hadoop.commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author Zenghu
 * @Date 2021/2/27 21:34
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendFirstReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer friends = new StringBuffer();
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            String friend = iterator.next().toString();
            if (iterator.hasNext()) {
                friends.append(friend).append(",");
            } else {
                friends.append(friend);
            }

        }
        context.write(key, new Text(friends.toString()));
    }
}
