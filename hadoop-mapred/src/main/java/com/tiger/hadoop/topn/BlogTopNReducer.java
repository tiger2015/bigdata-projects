package com.tiger.hadoop.topn;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @Author Zenghu
 * @Date 2021/2/28 20:52
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class BlogTopNReducer extends Reducer<IntWritable, Blog, Blog, IntWritable> {
    private TreeMap<Blog, Integer> topNTreeMap;
    private int n;
    private int total;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = context.getConfiguration().getInt("topN", 10);
        topNTreeMap = new TreeMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Blog> values, Context context) throws IOException, InterruptedException {
        for (Blog blog : values) {
            // 此处的数据要是用深拷贝
            // 由于在同一个线程中，values中的数据都会被修改
            // 加入后每次的值都会改变，而且造成topNTreeMap永远都只有一个值
            Blog replica = new Blog();
            replica.setUuid(blog.getUuid());
            replica.setFans(blog.getFans());
            replica.setAuthor(blog.getAuthor());
            replica.setFollows(blog.getFollows());
            topNTreeMap.put(replica, key.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Blog blog : topNTreeMap.keySet()) {
            context.write(blog, new IntWritable(topNTreeMap.get(blog)));
            count++;
            if (count > n) {
                break;
            }
        }
    }
}
