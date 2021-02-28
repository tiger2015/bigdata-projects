package com.tiger.hadoop.grouptopn;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 计算平均值
 * @Author Zenghu
 * @Date 2021/2/28 22:43
 * @Description
 * @Version: 1.0
 **/
public class ScoreAvgJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "cal-avg-job");
        job.setJarByClass(ScoreAvgJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data/scores/input/scores.txt"));

        job.setMapperClass(ScoreAvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(ScoreAvgReducer.class);

        job.setReducerClass(ScoreAvgReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        if (HDFSUtil.exists("/data/scores/out1")) {
            HDFSUtil.delete("/data/scores/out1", true);
        }
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/data/scores/out1"));

        boolean completion = job.waitForCompletion(true);

        return completion ? 0 : 1;
    }
}
