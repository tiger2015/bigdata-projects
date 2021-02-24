package com.tiger.hadoop.flowcountpartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowCountPartitioner.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(4);

        job.setOutputFormatClass(TextOutputFormat.class);

        LocalFileSystem fileSystem = FileSystem.getLocal(getConf());
        if (fileSystem.exists(new Path("file:///G:\\data\\flowcountpartition\\out"))) {
            fileSystem.delete(new Path("file:///G:\\data\\flowcountpartition\\out"), true);
        }
        FileInputFormat.setInputPaths(job, new Path("file:///G:\\data\\flowcount\\input"));
        FileOutputFormat.setOutputPath(job, new Path("file:///G:\\data\\flowcountpartition\\out"));

        boolean completion = job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new FlowCountJob(), args);
        System.exit(run);
    }
}
