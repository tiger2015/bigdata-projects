package com.tiger.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author Zenghu
 * @Date 2021/2/21 18:07
 * @Description
 * @Version: 1.0
 **/
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {


    /**
     * 返回分区号
     *
     * @param text
     * @param intWritable
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String s = text.toString();
        if (s.matches("[a-z]+")) { // 正常的单词
            return 0;
        } else {
            return 1;
        }
    }
}
