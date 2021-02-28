package com.tiger.hadoop.filemerge;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 10:43
 * @Description
 * @Version: 1.0
 **/
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecoreReader();
    }

    /**
     *
     * @param context
     * @param filename
     * @return false: 文件不能被分割
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // 文件不能被分割
        return false;
    }
}
