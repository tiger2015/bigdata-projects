package com.tiger.myflink;

import java.io.Serializable;

/**
 * @Author Zenghu
 * @Date 2021/5/24 21:47
 * @Description
 * @Version: 1.0
 **/
public class TempSensorData implements Serializable {
    private String id;
    private long time;
    private double temp;

    public TempSensorData() {
    }

    public TempSensorData(String id, long time, double temp) {
        this.id = id;
        this.time = time;
        this.temp = temp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "TempSensorData{" +
                "id='" + id + '\'' +
                ", time=" + time +
                ", temp=" + temp +
                '}';
    }
}
