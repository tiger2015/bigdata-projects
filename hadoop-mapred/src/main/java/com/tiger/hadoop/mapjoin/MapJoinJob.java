package com.tiger.hadoop.mapjoin;

import com.tiger.hadoop.hdfs.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @Author Zenghu
 * @Date 2021/2/27 17:23
 * @Description
 * @Version: 1.0
 **/
public class MapJoinJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("map_join_job");
        job.setJarByClass(MapJoinJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        // 添加到缓存中
        job.addCacheFile(new URI("hdfs://dev:9000/data/join/input/product.txt"));

        FileInputFormat.addInputPath(job, new Path("hdfs://dev:9000/data/join/input/order.txt"));

        if(HDFSUtil.exists("/data/join/out2")){
            HDFSUtil.delete("/data/join/out2", true);
        }

        FileOutputFormat.setOutputPath(job, new Path("hdfs://dev:9000/data/join/out2"));

        boolean completion = job.waitForCompletion(true);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new MapJoinJob(), args);
        System.exit(run);
    }

}
