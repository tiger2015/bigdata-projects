package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Zenghu
 * @Date 2021/2/28 22:48
 * @Description
 * @Version: 1.0
 **/
public class Grade implements WritableComparable<Grade> {
    private String course; // 课程名称
    private String student; // 学生
    private Double score;  // 分数

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getStudent() {
        return student;
    }

    public void setStudent(String student) {
        this.student = student;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public int compareTo(Grade o) {
        int result = course.compareTo(o.course);
        if (result == 0) {
            // 按照成绩降序排列
            return o.score.compareTo(score);
        }
        return result;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(student);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        student = in.readUTF();
        score = in.readDouble();
    }

    @Override
    public String toString() {
        return course + '\t' + student + '\t' + score;
    }

}
