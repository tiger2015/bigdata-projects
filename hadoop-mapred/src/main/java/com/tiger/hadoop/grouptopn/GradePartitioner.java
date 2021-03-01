package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author Zenghu
 * @Date 2021/2/28 22:51
 * @Description
 * @Version: 1.0
 **/
public class GradePartitioner extends Partitioner<Grade, Text> {


    @Override
    public int getPartition(Grade grade, Text text, int numPartitions) {
        return (grade.getCourse().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
