package com.tiger.hadoop.sort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义Writable
 *
 * @Author Zenghu
 * @Date 2021/2/21 21:20
 * @Description
 * @Version: 1.0
 **/
public class PairWritable implements WritableComparable {
    private String word;
    private double score;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public int compareTo(Object o) {
        PairWritable writable = (PairWritable) o;
        int result = this.word.compareTo(writable.word);
        if (result == 0) {
            return (int) (this.score - writable.score);
        }
        return result;
    }

    // 写数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeDouble(score);
    }

    // 读数据
    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        score = in.readDouble();
    }

    @Override
    public String toString() {
        return word + '\t' + score;
    }
}
