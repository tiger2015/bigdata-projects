package com.tiger.hadoop.flowsort;

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
public class FlowSortJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("flowsort");
        job.setJarByClass(FlowSortJob.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        LocalFileSystem fileSystem = FileSystem.getLocal(getConf());
        if (fileSystem.exists(new Path("file:///G:\\data\\flowsort\\out"))) {
            fileSystem.delete(new Path("file:///G:\\data\\flowsort\\out"), true);
        }
        FileInputFormat.setInputPaths(job, new Path("file:///G:\\data\\flowcount\\out"));
        FileOutputFormat.setOutputPath(job, new Path("file:///G:\\data\\flowsort\\out"));

        boolean completion = job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new FlowSortJob(), args);
        System.exit(run);
    }
}
