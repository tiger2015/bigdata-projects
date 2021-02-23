package com.tiger.hadoop.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Zenghu
 * @Date 2021/2/23 20:50
 * @Description
 * @Version: 1.0
 **/
public class FlowBean implements Writable {

    private int upPakcges;
    private int downPackages;
    private int upFlow;
    private int downFlow;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upPakcges);
        out.writeInt(downPackages);
        out.writeInt(upFlow);
        out.writeInt(downFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upPakcges = in.readInt();
        downPackages = in.readInt();
        upFlow = in.readInt();
        downFlow = in.readInt();
    }

    public int getUpPakcges() {
        return upPakcges;
    }

    public void setUpPakcges(int upPakcges) {
        this.upPakcges = upPakcges;
    }

    public int getDownPackages() {
        return downPackages;
    }

    public void setDownPackages(int downPackages) {
        this.downPackages = downPackages;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return upPakcges + "\t" + downPackages + "\t" + upFlow + "\t" + downFlow;
    }
}
