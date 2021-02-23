package com.tiger.hadoop.wordcount;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.lang.model.SourceVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * @Author Zenghu
 * @Date 2021/2/18 0:02
 * @Description
 * @Version: 1.0
 **/
public class WordCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "tiger");
        Configuration conf = new Configuration();
        conf.set("yarn.resourcemanager.hostname", "dev");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.defaultFS", "hdfs://dev:9000/");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        int run = ToolRunner.run(conf, new WordCountJob(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(WordCountJob.class);
        job.setJar("C:\\Users\\ZengHu\\Desktop\\hadoop-mapred-1.0.jar");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setPartitionerClass(WordCountPartitioner.class);
        // 设置ReduceTask的数量
        job.setNumReduceTasks(2);

        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/data"));
        Path output = new Path("/result");
        if (HDFSUtil.exists("/result"))
            HDFSUtil.delete("/result", true);
        FileOutputFormat.setOutputPath(job, output);
        boolean completion = job.waitForCompletion(true);
        if (completion) {
            System.out.println("finish");
        } else {
            System.out.println("error");
        }
        return 0;
    }
}
