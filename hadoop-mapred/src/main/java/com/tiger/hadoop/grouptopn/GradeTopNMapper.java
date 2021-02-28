package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/28 22:54
 * @Description
 * @Version: 1.0
 **/
public class GradeTopNMapper extends Mapper<LongWritable, Text, Grade, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String[] split1 = split[0].split(",");
        Grade grade = new Grade();
        grade.setCourse(split1[0]);
        grade.setStudent(split1[1]);
        grade.setScore(Double.valueOf(split[1]));
        context.write(grade, value);
    }
}
