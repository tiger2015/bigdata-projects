package com.tiger.hadoop.filemerge;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 16:50
 * @Description
 * @Version: 1.0
 **/
public class FileMergeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private String fileName = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(fileName), value);
    }
}
