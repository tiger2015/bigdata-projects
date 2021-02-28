package com.tiger.hadoop.commonfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author Zenghu
 * @Date 2021/2/27 22:28
 * @Description
 * @Version: 1.0
 **/
public class CommonFriendJob {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new CommonFriendFirstJob(), args);
        if (run == 0) {
            run = ToolRunner.run(configuration, new CommomFriendSecondJob(), args);
        }
        System.exit(run);
    }
}
