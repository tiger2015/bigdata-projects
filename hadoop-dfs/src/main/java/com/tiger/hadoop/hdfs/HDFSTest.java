package com.tiger.hadoop.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @Author Zenghu
 * @Date 2021/2/6 17:43
 * @Description
 * @Version: 1.0
 **/
@Slf4j
public class HDFSTest {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        /**
         System.setProperty("HADOOP_USER_NAME", "root");
         Configuration conf = new Configuration();
         conf.set("fs.defaultFS", "hdfs://172.18.4.1:9000");
         conf.set("dfs.replication","1");
         conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
         try (FileSystem fileSystem = FileSystem.get(conf)) {
         Path src = new Path("C:\\Users\\ZengHu\\Desktop\\test.txt");
         Path dest = new Path("/data/test.txt");
         fileSystem.copyFromLocalFile(src, dest);
         } catch (IOException e) {
         e.printStackTrace();
         }
         **/

       HDFSUtil.download("/data/commonFriend/out2", "C:\\Users\\ZengHu\\\\Desktop\\");

       //HDFSUtil.upload("C:\\Users\\ZengHu\\Desktop\\friends.txt", "/data/commonFriend/input/friends.txt");



        /**
        List<FileStatus> list = HDFSUtil.list("/", true);
        for (FileStatus fileStatus : list) {
            log.info("file:{}", fileStatus.toString());
        }
        **/
    }


}
