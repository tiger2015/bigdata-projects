package com.tiger.hadoop.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @Author Zenghu
 * @Date 2021/2/28 20:42
 * @Description
 * @Version: 1.0
 **/
public class BlogTopNMapper extends Mapper<LongWritable, Text, IntWritable, Blog> {

    private TreeMap<Blog, String> topNBlogs = new TreeMap<>();
    private int n;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = context.getConfiguration().getInt("topN", 10);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        Blog blog = new Blog();
        blog.setUuid(split[0]);
        blog.setPublishDate(split[1]);
        blog.setLikes(Integer.parseInt(split[2]));
        blog.setComments(Integer.parseInt(split[3]));
        blog.setRelays(Integer.parseInt(split[4]));
        blog.setAuthor(split[5]);
        blog.setFollows(Integer.parseInt(split[6]));
        blog.setFans(Integer.parseInt(split[7]));
        blog.setBlogerSex(split[8]);
        blog.setContent(split[9]);
        topNBlogs.put(blog, blog.getUuid());
        if (topNBlogs.size() > n) {
            topNBlogs.remove(topNBlogs.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int index = 0;
        for (Blog blog : topNBlogs.keySet()) {
            context.write(new IntWritable(index++), blog);
        }
    }
}
