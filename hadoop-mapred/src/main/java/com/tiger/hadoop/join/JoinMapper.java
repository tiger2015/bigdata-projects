package com.tiger.hadoop.join;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/26 22:40
 * @Description
 * @Version: 1.0
 **/
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String inputFileName;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        Path path = inputSplit.getPath();
        inputFileName = path.getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if (inputFileName.equals("order.txt")) {
            context.write(new Text(split[1]), value);
        } else {
            context.write(new Text(split[0]), value);
        }
    }
}
