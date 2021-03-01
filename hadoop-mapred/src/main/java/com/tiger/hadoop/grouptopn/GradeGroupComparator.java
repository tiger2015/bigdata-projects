package com.tiger.hadoop.grouptopn;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * 按照课程名称分组
 * @Author Zenghu
 * @Date 2021/2/28 22:56
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class GradeGroupComparator extends WritableComparator {


    public GradeGroupComparator() {
        super(Grade.class, true);
    }


    /**
     * 注意是重写compare(WritableComparable a, WritableComparable b)接口
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Grade first = (Grade) a;
        Grade second = (Grade) b;
        return first.getCourse().compareTo(second.getCourse());
    }
}
