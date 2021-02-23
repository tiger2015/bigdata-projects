package com.tiger.hadoop.sort;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author Zenghu
 * @Date 2021/2/21 21:29
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class SortJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "sort");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SortMapper.class);

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("file:///C:\\Users\\ZengHu\\Desktop\\sort\\input"));

        FileOutputFormat.setOutputPath(job, new Path("file:///C:\\Users\\ZengHu\\Desktop\\sort\\output"));

        boolean completion = job.waitForCompletion(true);
        if (completion) {
            log.info("=========finish===========");
        } else {
            log.warn("=========error============");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new SortJob(), args);
        System.exit(run);
    }
}
