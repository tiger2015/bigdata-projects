package com.tiger.hadoop.grouptopn;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/28 23:04
 * @Description
 * @Version: 1.0
 **/
public class GradeTopNJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "topN-job");
        job.setJarByClass(GradeTopNJob.class);
        // 设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data/scores/out1"));

        // 设置Mapper
        job.setMapperClass(GradeTopNMapper.class);
        job.setMapOutputKeyClass(Grade.class);
        job.setMapOutputValueClass(Text.class);
        // 分区
        job.setPartitionerClass(GradePartitioner.class);
        // 排序
        // 规约
        // 分组
        job.setGroupingComparatorClass(GradeGroupComparator.class);

        // 设置Reducer
        job.setReducerClass(GradeTopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输出
        job.setOutputFormatClass(TextOutputFormat.class);

        if (HDFSUtil.exists("/data/scores/out2")) {
            HDFSUtil.delete("/data/scores/out2", true);
        }
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/data/scores/out2"));

        boolean completion = job.waitForCompletion(true);

        return completion ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("top.n", 3);
        ToolRunner.run(configuration, new GradeTopNJob(), args);

    }
}
