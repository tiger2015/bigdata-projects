package com.tiger.hadoop.grouptopn;


import lombok.extern.slf4j.Slf4j;
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

    @Override
    public int compare(Object a, Object b) {
        Grade first = (Grade) a;
        Grade second = (Grade) b;
        return first.getCourse().compareTo(second.getCourse());
    }
}
