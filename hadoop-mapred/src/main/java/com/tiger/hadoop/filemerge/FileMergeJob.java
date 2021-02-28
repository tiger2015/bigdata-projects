package com.tiger.hadoop.filemerge;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/28 16:54
 * @Description
 * @Version: 1.0
 **/
public class FileMergeJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "merge_file_job");
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data"));
        MyInputFormat.setInputDirRecursive(job, true); // 如果输入的是目录，则遍历目录下的所有文件

        job.setMapperClass(FileMergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);


        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if (HDFSUtil.exists("/mergeData")) {
            HDFSUtil.delete("/mergeData", true);
        }
        SequenceFileOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/mergeData"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        boolean completion = job.waitForCompletion(true);
        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://dev:9000");
        int run = ToolRunner.run(configuration, new FileMergeJob(), args);
        System.exit(run);
    }

}
