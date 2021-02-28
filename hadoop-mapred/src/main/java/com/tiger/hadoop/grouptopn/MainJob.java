package com.tiger.hadoop.grouptopn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/28 23:10
 * @Description
 * @Version: 1.0
 **/
public class MainJob {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("top.n", 3);
        int run = ToolRunner.run(configuration, new ScoreAvgJob(), args);
        if (run == 0) {
            ToolRunner.run(configuration, new GradeTopNJob(), args);
        }
    }
}
