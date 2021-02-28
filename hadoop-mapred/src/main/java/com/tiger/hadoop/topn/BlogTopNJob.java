package com.tiger.hadoop.topn;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/28 20:58
 * @Description
 * @Version: 1.0
 **/
public class BlogTopNJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "topN-job");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data/blogs/input"));

        job.setMapperClass(BlogTopNMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Blog.class);

        job.setReducerClass(BlogTopNReducer.class);
        job.setOutputKeyClass(Blog.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputFormatClass(TextOutputFormat.class);
        if (HDFSUtil.exists("/data/blogs/out")) {
            HDFSUtil.delete("/data/blogs/out", true);
        }

        TextOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/data/blogs/out"));



        boolean completion = job.waitForCompletion(true);

        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("topN", 10);

        int run = ToolRunner.run(configuration, new BlogTopNJob(), args);
        System.exit(run);
    }
}
