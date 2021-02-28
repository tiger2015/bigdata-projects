package com.tiger.hadoop.commonfriends;

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
 * @Date 2021/2/27 22:10
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendFirstJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("common-friend-job1");


        job.setMapperClass(CommonFriendFirstMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CommonFriendFirstReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data/commonFriend/input"));
        if (HDFSUtil.exists("/data/commonFriend/out1")) {
            HDFSUtil.delete("/data/commonFriend/out1", true);
        }
        FileOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/data/commonFriend/out1"));


        boolean completion = job.waitForCompletion(true);

        if (completion) {
            System.out.println("job success finish");
            return 0;
        } else {
            System.out.println("job fail");
            return 1;
        }
    }
}
