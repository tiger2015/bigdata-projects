package com.tiger.hadoop.flowcount;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/23 21:01
 * @Description
 * @Version: 1.0
 **/
public class FlowCountJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("flowcount");
        job.setJarByClass(FlowCountJob.class);
        job.setJar("C:\\Users\\ZengHu\\Desktop\\hadoop-mapred-1.0.jar");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        if (HDFSUtil.exists("/data/flow/out")) {
            HDFSUtil.delete("/data/flow/out", true);
        }
        FileInputFormat.setInputPaths(job, new Path("/data/flow/input"));
        FileOutputFormat.setOutputPath(job, new Path("/data/flow/out"));

        boolean completion = job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "tiger");
        Configuration configuration = new Configuration();
        configuration.set("yarn.resourcemanager.hostname", "dev");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("fs.defaultFS", "hdfs://dev:9000/");
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        int run = ToolRunner.run(configuration, new FlowCountJob(), args);
        System.exit(run);
    }
}
